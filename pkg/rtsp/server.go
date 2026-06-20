package rtsp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"tuya-ipc-terminal/pkg/core"
	"tuya-ipc-terminal/pkg/storage"
	"tuya-ipc-terminal/pkg/tuya"
)

type RTSPServer struct {
	port           int
	listener       net.Listener
	storageManager *storage.StorageManager
	clients        map[string]*RTSPClient
	streams        map[string]*CameraStream
	mutex          sync.RWMutex
	ctx            context.Context
	cancel         context.CancelFunc
	running        bool
}

type RTSPClient struct {
	conn                 net.Conn
	session              string
	cameraPath           string
	stream               *CameraStream
	reader               *bufio.Reader
	transportMode        TransportMode
	videoRTPPort         int
	videoRTCPPort        int
	audioRTPPort         int
	audioRTCPPort        int
	backAudioRTPPort     int // server-side port for back audio
	backAudioRTCPPort    int // server-side port for back audio RTCP
	videoRTPChannel      byte
	videoRTCPChannel     byte
	audioRTPChannel      byte
	audioRTCPChannel     byte
	backAudioRTPChannel  byte
	backAudioRTCPChannel byte
	setupCount           int
}

type CameraStream struct {
	camera       *storage.CameraInfo
	resolution   string
	user         *storage.UserSession
	webrtcBridge *WebRTCBridge
	clients      map[string]*RTSPClient
	mutex        sync.RWMutex
	connecting   bool
	active       bool
	lastActivity time.Time

	// Delayed shutdown
	shutdownTimer *time.Timer
	shutdownDelay time.Duration

	// Reference to server for cleanup
	server   *RTSPServer
	streamId string

	// Startup coordination (avoid races where PLAY happens before WebRTC startup
	// actually begins, leaving clients with 0 packets or spurious 503s).
	startOnce sync.Once
	readyCh   chan struct{} // closed when WebRTC is connected

	// Retry backoff: when a corrupt-H264 camera fails WebRTC instantly, we
	// don't want to retry every 500ms for 40s. Each attempt creates ICE
	// candidate UDP sockets that enter TIME_WAIT, accumulating 1000+ sockets
	// in minutes. Set this after each failure to skip retries until it passes.
	backoffDeadline time.Time
}


type ServerConfig struct {
	Port                 int
	MaxClients           int
	StreamTimeout        time.Duration
	ConnectionTimeout    time.Duration
	EnableAuthentication bool
}

func NewRTSPServer(port int, storageManager *storage.StorageManager) *RTSPServer {
	ctx, cancel := context.WithCancel(context.Background())

	return &RTSPServer{
		port:           port,
		storageManager: storageManager,
		clients:        make(map[string]*RTSPClient),
		streams:        make(map[string]*CameraStream),
		ctx:            ctx,
		cancel:         cancel,
		running:        false,
	}
}

func (s *RTSPServer) Start() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.running {
		return errors.New("server is already running")
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %v", s.port, err)
	}

	s.listener = listener
	s.running = true

	core.Logger.Info().Msgf("RTSP Server started on port %d", s.port)
	core.Logger.Info().Msgf("Available endpoints:")

	// List available camera endpoints
	if err := s.printAvailableEndpoints(); err != nil {
		core.Logger.Warn().Msgf("Could not list camera endpoints: %v", err)
	}

	// Start accepting connections
	go s.acceptConnections()

	// Start cleanup routine
	go s.cleanupRoutine()

	return nil
}

func (s *RTSPServer) Stop() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.running {
		return errors.New("server is not running")
	}

	core.Logger.Info().Msg("Stopping RTSP server...")

	// Cancel context to stop all goroutines
	s.running = false
	s.cancel()

	// Close listener
	if s.listener != nil {
		s.listener.Close()
	}

	// Close all client connections
	for _, client := range s.clients {
		client.conn.Close()
	}

	// Stop all streams
	for _, stream := range s.streams {
		stream.Stop()
	}

	return nil
}

func (s *RTSPServer) IsRunning() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.running
}

func (s *RTSPServer) GetPort() int {
	return s.port
}

func (s *RTSPServer) GetStats() ServerStats {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	activeStreams := 0
	for _, stream := range s.streams {
		if stream.active {
			activeStreams++
		}
	}

	return ServerStats{
		Port:         s.port,
		Running:      s.running,
		ClientCount:  len(s.clients),
		StreamCount:  activeStreams,
		TotalStreams: len(s.streams),
	}
}

type ServerStats struct {
	Port         int  `json:"port"`
	Running      bool `json:"running"`
	ClientCount  int  `json:"clientCount"`
	StreamCount  int  `json:"activeStreamCount"`
	TotalStreams int  `json:"totalStreams"`
}

func (s *RTSPServer) acceptConnections() {
	for s.running {
		select {
		case <-s.ctx.Done():
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				if s.running {
					core.Logger.Error().Err(err).Msg("Error accepting connection")
				}
				continue
			}

			// Handle connection in goroutine
			go s.handleConnection(conn)
		}
	}
}

func (s *RTSPServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	session := generateSessionID()
	core.Logger.Info().Msgf("New RTSP connection established, session=%s", session)

	reader := bufio.NewReader(conn)

	// Parse initial RTSP request
	request, err := s.parseRTSPRequestFromReader(reader)
	if err != nil {
		core.Logger.Error().Err(err).Msg("Error parsing initial RTSP request")
		return
	}

	// Extract camera path from URL
	cameraPath, streamResolution := extractCameraPath(request.URL)
	if cameraPath == "" {
		core.Logger.Error().Msg("Invalid RTSP URL")
		sendRTSPResponse(conn, 400, "Bad Request", nil, "")
		return
	}

	// Find camera
	camera, user, err := s.findCamera(cameraPath)
	if err != nil {
		core.Logger.Error().Msgf("Error finding camera for path %s: %v", cameraPath, err)
		sendRTSPResponse(conn, 500, "Internal Server Error", nil, "")
		return
	}

	if camera == nil {
		core.Logger.Error().Msgf("Camera not found for path %s", cameraPath)
		sendRTSPResponse(conn, 404, "Not Found", nil, "")
		return
	}

	core.Logger.Info().Msgf("New RTSP connection for camera: %s (%s)", camera.DeviceName, camera.DeviceID)

	// Create or get existing stream
	stream, err := s.getOrCreateStream(camera, streamResolution, user)
	if err != nil {
		core.Logger.Error().Err(err).Msgf("Failed to create stream for camera %s", camera.DeviceName)
		sendRTSPResponse(conn, 500, "Internal Server Error", nil, "Failed to create stream")
		return
	}

	// Create RTSP client
	client := &RTSPClient{
		conn:                conn,
		reader:              reader,
		session:             session,
		cameraPath:          cameraPath,
		stream:              stream,
		transportMode:       TransportUDP, // Default to UDP
		videoRTPPort:        0,
		audioRTPPort:        0,
		backAudioRTPPort:    0,
		videoRTPChannel:     0,
		audioRTPChannel:     2,
		backAudioRTPChannel: 4,
		setupCount:          0,
	}

	// Add client to server and stream
	s.addClient(client)
	stream.AddClient(client)

	// Handle initial request
	s.handleRTSPMethod(client, request)

	// Handle further requests
	s.handleRTSPProtocol(client)
}

func (s *RTSPServer) findCamera(path string) (*storage.CameraInfo, *storage.UserSession, error) {
	cameras, err := s.storageManager.GetAllCameras()
	if err != nil {
		return nil, nil, err
	}

	// Find camera by RTSP path
	for _, camera := range cameras {
		if camera.RTSPPath == path {
			// Get user for this camera
			users, err := s.storageManager.ListUsers()
			if err != nil {
				continue
			}

			for _, user := range users {
				if user.UserKey == camera.UserKey {
					return &camera, &user, nil
				}
			}
		}
	}

	return nil, nil, nil
}

func (s *RTSPServer) getOrCreateStream(camera *storage.CameraInfo, streamResolution string, user *storage.UserSession) (*CameraStream, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Check if stream already exists
	streamId := fmt.Sprintf("%s-%s", camera.DeviceID, streamResolution)
	if stream, exists := s.streams[streamId]; exists {
		if stream.active || stream.connecting {
			core.Logger.Trace().Msgf("Reusing existing stream for camera: %s", camera.DeviceName)
			stream.lastActivity = time.Now()
			return stream, nil
		}
	}

	// Create new stream
	// Create new stream; connecting should only be set by startStream().
	stream := NewCameraStream(camera, streamResolution, user, s.storageManager, s)

	stream.webrtcBridge.OnError = func(err error) {
		core.Logger.Error().Err(err).Msgf("WebRTC error for camera %s: stopping stream to free UDP sockets (active=%v connecting=%v)", camera.DeviceName, stream.active, stream.connecting)

		// Always stop the stream on WebRTC failure, regardless of active
		// clients or connection state. The dead peer connection's UDP sockets
		// (ICE, STUN, TURN relay channels) remain open until
		// peerConnection.Close() is called, and stopping is the only path
		// that triggers that cleanup. Without this unconditional call, the
		// zombie bridge accumulates thousands of UDP sockets over hours of
		// operation, eventually bogging down the network stack.
		//
		// When both active=false AND connecting=false (e.g. between
		// EnsureStarted reconnection attempts), the old guard skipped cleanup
		// entirely, causing catastrophic socket accumulation for corrupt-H264
		// cameras that reconnect rapidly.
		//
		// Active RTSP clients will reconnect through getOrCreateStream ->
		// startStream -> new WebRTC bridge -> fresh peer connection.
		stream.mutex.Lock()
		stream.stopStreamInternal()
		stream.mutex.Unlock()
	}

	s.streams[streamId] = stream

	core.Logger.Info().Msgf("Created new stream for camera: %s", camera.DeviceName)
	return stream, nil
}

func (s *RTSPServer) removeStream(streamId string, expected *CameraStream) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if existing, exists := s.streams[streamId]; exists {
		// Only delete if the map entry still points to the expected stream.
		// The async goroutine in stopStreamInternal can race with
		// getOrCreateStream, which replaces the entry for the same key.
		// Without this guard, a stale goroutine deletes the newly created
		// stream (Bug 1 in the 6th leak path).
		if existing == expected || expected == nil {
			delete(s.streams, streamId)
			core.Logger.Trace().Msgf("Removed stream %s from server map", streamId)
		}
	}
}

func (s *RTSPServer) addClient(client *RTSPClient) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.clients[client.session] = client
}

func (s *RTSPServer) removeClient(sessionID string) {
	s.mutex.Lock()
	client, exists := s.clients[sessionID]
	if exists {
		delete(s.clients, sessionID)
	}
	s.mutex.Unlock()

	if !exists {
		return
	}

	// Do not hold the server mutex while removing from the stream. Stream removal
	// can wait for a WebRTC startup/shutdown lock, and holding the server mutex
	// there blocks unrelated RTSP connections from being accepted/served.
	if client.stream != nil {
		client.stream.RemoveClient(sessionID)
	}
	client.conn.Close()
}

func (s *RTSPServer) printAvailableEndpoints() error {
	cameras, err := s.storageManager.GetAllCameras()
	if err != nil {
		return err
	}

	if len(cameras) == 0 {
		core.Logger.Warn().Msg("  No cameras available. Run 'cameras refresh' first.")
		return nil
	}

	for _, camera := range cameras {
		var skill *tuya.Skill
		json.Unmarshal([]byte(camera.Skill), &skill)

		supportClarity := skill != nil && (skill.WebRTC&(1<<5)) != 0
		baseUrl := fmt.Sprintf("rtsp://localhost:%d%s", s.port, camera.RTSPPath)

		if supportClarity {
			core.Logger.Info().Msgf("  %s/hd (%s)", baseUrl, camera.DeviceName)
			core.Logger.Info().Msgf("  %s/sd (%s)", baseUrl, camera.DeviceName)
		} else {
			core.Logger.Info().Msgf("  %s (%s)", baseUrl, camera.DeviceName)
		}
	}

	return nil
}

func (s *RTSPServer) cleanupRoutine() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.cleanupInactiveStreams()
		}
	}
}

func (s *RTSPServer) cleanupInactiveStreams() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	now := time.Now()
	for deviceID, stream := range s.streams {
		// Remove streams inactive for more than 5 minutes
		if now.Sub(stream.lastActivity) > 5*time.Minute && len(stream.clients) == 0 {
			core.Logger.Trace().Msgf("Cleaning up inactive stream for camera: %s", stream.camera.DeviceName)
			stream.Stop()
			delete(s.streams, deviceID)
		}
	}
}

func NewCameraStream(camera *storage.CameraInfo, resolution string, user *storage.UserSession, storageManager *storage.StorageManager, server *RTSPServer) *CameraStream {
	stream := &CameraStream{
		camera:        camera,
		resolution:    resolution,
		user:          user,
		clients:       make(map[string]*RTSPClient),
		active:        false,
		lastActivity:  time.Now(),
		shutdownDelay: 5 * time.Second,
		server:        server,
		streamId:      fmt.Sprintf("%s-%s", camera.DeviceID, resolution),
		readyCh:       make(chan struct{}),
	}

	stream.webrtcBridge = NewWebRTCBridge(camera, resolution, user, storageManager)

	return stream
}

func (cs *CameraStream) AddClient(client *RTSPClient) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	// Cancel any pending shutdown
	if cs.shutdownTimer != nil {
		cs.shutdownTimer.Stop()
		cs.shutdownTimer = nil
		core.Logger.Trace().Msgf("Cancelled pending shutdown for camera %s - new client connected", cs.camera.DeviceName)
	}

	cs.clients[client.session] = client
	cs.lastActivity = time.Now()

	// Start stream if not active
	if !cs.active {
		go cs.startStream()
	}
}

func (cs *CameraStream) RemoveClient(sessionID string) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	// Remove from RTP forwarder
	if cs.webrtcBridge != nil && cs.webrtcBridge.rtpForwarder != nil {
		cs.webrtcBridge.rtpForwarder.RemoveClient(sessionID)
	}

	delete(cs.clients, sessionID)
	cs.lastActivity = time.Now()

	// Schedule stream shutdown if no clients and stream is active
	if len(cs.clients) == 0 && cs.active {
		cs.scheduleShutdown()
	}
}

func (cs *CameraStream) SetShutdownDelay(delay time.Duration) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.shutdownDelay = delay
}

func (cs *CameraStream) Stop() {
	// Clear all clients first
	for sessionID := range cs.clients {
		cs.RemoveClient(sessionID)
	}

	// Stop the stream
	cs.stopStream()
}

func (cs *CameraStream) startStream() {
	// IMPORTANT: do not hold cs.mutex while doing the potentially long-running
	// WebRTC startup. Holding the mutex blocks AddClient/RTSP request handling and
	// can make the RTSP server appear hung (e.g. /sd OPTIONS never gets a response).
	cs.mutex.Lock()
	if cs.active || cs.connecting {
		cs.mutex.Unlock()
		return
	}
	// Respect retry backoff: if WebRTC failed recently (corrupt H264), skip
	// this attempt. The caller should try again after the deadline passes.
	if time.Now().Before(cs.backoffDeadline) {
		core.Logger.Debug().Msgf("Skipping startStream for %s — retry backoff active until %v", cs.camera.DeviceName, cs.backoffDeadline)
		cs.mutex.Unlock()
		return
	}
	cs.connecting = true
	cs.mutex.Unlock()

	core.Logger.Info().Msgf("Starting stream for camera: %s (streamId=%s webrtc=%p)", cs.camera.DeviceName, cs.streamId, cs.webrtcBridge)

	// Guard against wedging forever inside WebRTC startup.
	// If this hangs, the RTSP side gets stuck with connecting=true and PLAY will 503 indefinitely.
	startErrCh := make(chan error, 1)
	go func() {
		startErrCh <- cs.webrtcBridge.Start()
	}()

	select {
	case err := <-startErrCh:
		if err != nil {
			core.Logger.Error().Err(err).Msg("Failed to start WebRTC bridge")
			cs.mutex.Lock()
			// Set retry backoff: don't try again for 30s so corrupt
			// cameras don't burn through ICE candidate sockets in a
			// tight spin loop. Each attempt allocates UDP sockets
			// that enter TIME_WAIT — enough cycles and they pile up.
			cs.backoffDeadline = time.Now().Add(30 * time.Second)
			cs.stopStreamInternal()
			cs.mutex.Unlock()
			return
		}
	case <-time.After(40 * time.Second):
		core.Logger.Error().Msgf("Timed out starting WebRTC bridge for camera: %s", cs.camera.DeviceName)
		cs.mutex.Lock()
		cs.stopStreamInternal()
		cs.mutex.Unlock()
		return
	}

	cs.mutex.Lock()
	cs.connecting = false
	cs.active = true
	readyCh := cs.readyCh
	cs.mutex.Unlock()

	// Signal readiness (WebRTC is connected and stream is active)
	select {
	case <-readyCh:
		// already closed
	default:
		close(readyCh)
	}
}

func (cs *CameraStream) stopStream() {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.stopStreamInternal()
}

func (cs *CameraStream) stopStreamInternal() {
	// Check if we should actually stop
	if !cs.active && !cs.connecting {
		return
	}

	wasActive := cs.active
	cs.active = false
	cs.connecting = false

	// Cancel any pending shutdown
	if cs.shutdownTimer != nil {
		cs.shutdownTimer.Stop()
		cs.shutdownTimer = nil
	}

	// Only log if we were actually active
	if wasActive {
		core.Logger.Info().Msgf("Stopping stream for camera: %s", cs.camera.DeviceName)
	}

	// Stop WebRTC bridge
	if cs.webrtcBridge != nil {
		cs.webrtcBridge.Stop()
	}

	// Reset readiness channel for future starts
	cs.readyCh = make(chan struct{})
	cs.startOnce = sync.Once{}

	// Remove from server map in a separate goroutine to avoid potential deadlock
	// with cleanupInactiveStreams which holds s.mutex and calls Stop().
	// Pass cs as the expected pointer so stale goroutines can't delete a
	// newer stream that replaced this entry (Bug 1 fix).
	go func() {
		if cs.server != nil {
			cs.server.removeStream(cs.streamId, cs)
		}
	}()
}

func (cs *CameraStream) scheduleShutdown() {
	// Don't schedule if we're not active
	if !cs.active {
		return
	}

	// Cancel any existing timer
	if cs.shutdownTimer != nil {
		cs.shutdownTimer.Stop()
	}

	core.Logger.Trace().Msgf("Scheduling shutdown for camera %s in %v", cs.camera.DeviceName, cs.shutdownDelay)

	cs.shutdownTimer = time.AfterFunc(cs.shutdownDelay, func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()

		// Double-check no clients connected during the delay and stream is still active
		if len(cs.clients) == 0 && cs.active {
			core.Logger.Info().Msgf("Executing delayed shutdown for camera %s", cs.camera.DeviceName)
			cs.stopStreamInternal()
		}

		cs.shutdownTimer = nil
	})
}

// EnsureStarted makes sure the underlying WebRTC bridge is running.
// This avoids answering RTSP PLAY before we actually have a media source.
func (cs *CameraStream) EnsureStarted(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		cs.mutex.Lock()
		active := cs.active
		connecting := cs.connecting
		wb := cs.webrtcBridge
		readyCh := cs.readyCh
		cs.mutex.Unlock()

		// Ready as soon as WebRTC is connected (or stream marked active).
		if active {
			return true
		}
		if wb != nil && wb.IsConnected() {
			return true
		}

		// Kick off startup if nothing is running.
		if !connecting {
			cs.mutex.Lock()
			inBackoff := time.Now().Before(cs.backoffDeadline)
			cs.mutex.Unlock()
			if inBackoff {
				// In retry backoff after a recent failure; wait for it to expire
				// rather than spawning another PeerConnection/ICE cycle that will
				// just create TIME_WAIT sockets and fail again.
				if deadline.After(time.Now()) {
					select {
					case <-readyCh:
						return true
					case <-time.After(500 * time.Millisecond):
						continue
					}
				}
			}
			// Rate-limit: wait 500ms after spawning startStream to avoid
			// creating 10 PeerConnections/sec when a corrupt stream fails
			// instantly. Each startStream allocates ICE candidate UDP sockets
			// that enter TIME_WAIT on close, accumulating 1000+ sockets in
			// seconds without this backoff.
			go cs.startStream()
			if deadline.After(time.Now()) {
				select {
				case <-readyCh:
					return true
				case <-time.After(500 * time.Millisecond):
					continue
				}
			}
		}

		if time.Now().After(deadline) {
			return false
		}

		// Wait briefly for readiness signal, then re-check.
		select {
		case <-readyCh:
			return true
		case <-time.After(100 * time.Millisecond):
		}
	}
}
