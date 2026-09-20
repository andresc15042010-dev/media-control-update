/**
 * supabase-remote.js
 * Cliente Liviano y Robusto para Control Remoto Global por Internet vía Supabase Realtime
 * Funciona en Navegador Móvil (4G/5G) y en la App de Escritorio Electron
 * Cero dependencias externas - Utiliza WebSocket nativo (Protocolo Phoenix Channels de Supabase)
 */

(function(window) {
    // Configuración por defecto (Proyecto Supabase con Realtime activo)
    const DEFAULT_CONFIG = {
        supabaseUrl: 'https://ojqbbhykuhdhlwazfeyz.supabase.co',
        anonKey: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9qcWJiaHlrdWhkaGx3YXpmZXl6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDI2MDQ4MDAsImV4cCI6MjA1ODE4MDgwMH0.placeholder',
        channelPrefix: 'mediacontrol_'
    };

    class SupabaseRealtimeRemote {
        constructor(options = {}) {
            this.url = options.supabaseUrl || localStorage.getItem('mc_supabase_url') || DEFAULT_CONFIG.supabaseUrl;
            this.anonKey = options.anonKey || localStorage.getItem('mc_supabase_anon_key') || DEFAULT_CONFIG.anonKey;
            this.pin = options.pin || localStorage.getItem('mc_church_pin') || this.generateDefaultPin();
            this.channelName = DEFAULT_CONFIG.channelPrefix + this.pin.toLowerCase().replace(/[^a-z0-9]/g, '');
            this.ws = null;
            this.heartbeatTimer = null;
            this.reconnectTimer = null;
            this.refCounter = 1;
            this.isConnected = false;
            this.isConnecting = false;
            this.onCommandCallback = null;
            this.onStatusCallback = null;
            this.onStateChangeCallback = null;
        }

        generateDefaultPin() {
            // Genera un PIN memorable de 6 caracteres (ej. MC7492)
            const randomNum = Math.floor(1000 + Math.random() * 9000);
            return 'MC' + randomNum;
        }

        setPin(pin) {
            if (!pin) return;
            this.pin = pin.toUpperCase().trim();
            this.channelName = DEFAULT_CONFIG.channelPrefix + this.pin.toLowerCase().replace(/[^a-z0-9]/g, '');
            try { localStorage.setItem('mc_church_pin', this.pin); } catch(e) {}
            if (this.isConnected || this.isConnecting) {
                this.reconnect();
            }
        }

        getPin() {
            return this.pin;
        }

        onCommand(cb) {
            this.onCommandCallback = cb;
        }

        onStatus(cb) {
            this.onStatusCallback = cb;
        }

        onStateChange(cb) {
            this.onStateChangeCallback = cb;
        }

        notifyState(state, info) {
            if (this.onStateChangeCallback) {
                this.onStateChangeCallback(state, info);
            }
        }

        connect() {
            if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
                return;
            }

            this.isConnecting = true;
            this.notifyState('connecting', { pin: this.pin });

            try {
                // Endpoint estándar de Phoenix WebSocket en Supabase Realtime
                const baseWsUrl = this.url.replace(/^http/, 'ws').replace(/\/$/, '');
                const wsUrl = `${baseWsUrl}/realtime/v1/websocket?apikey=${encodeURIComponent(this.anonKey)}&vsn=1.0.0`;

                this.ws = new WebSocket(wsUrl);

                this.ws.onopen = () => {
                    this.isConnected = true;
                    this.isConnecting = false;
                    this.notifyState('connected', { pin: this.pin, channel: this.channelName });
                    this.joinChannel();
                    this.startHeartbeat();
                };

                this.ws.onmessage = (event) => {
                    try {
                        const msg = JSON.parse(event.data);
                        this.handleMessage(msg);
                    } catch (e) {
                        console.warn('[SUPABASE_REALTIME] Error parseando mensaje:', e);
                    }
                };

                this.ws.onerror = (err) => {
                    console.warn('[SUPABASE_REALTIME] WebSocket error:', err);
                };

                this.ws.onclose = () => {
                    this.isConnected = false;
                    this.isConnecting = false;
                    this.stopHeartbeat();
                    this.notifyState('disconnected', { pin: this.pin });
                    this.scheduleReconnect();
                };
            } catch (err) {
                console.error('[SUPABASE_REALTIME] Error iniciando conexión:', err);
                this.isConnecting = false;
                this.scheduleReconnect();
            }
        }

        joinChannel() {
            if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
            const ref = String(this.refCounter++);
            const joinMsg = {
                topic: 'realtime:' + this.channelName,
                event: 'phx_join',
                payload: {
                    config: {
                        broadcast: { ack: false, self: false },
                        presence: { key: '' }
                    }
                },
                ref: ref
            };
            this.ws.send(JSON.stringify(joinMsg));
        }

        startHeartbeat() {
            this.stopHeartbeat();
            this.heartbeatTimer = setInterval(() => {
                if (this.ws && this.ws.readyState === WebSocket.OPEN) {
                    const hb = {
                        topic: 'phoenix',
                        event: 'heartbeat',
                        payload: {},
                        ref: String(this.refCounter++)
                    };
                    this.ws.send(JSON.stringify(hb));
                }
            }, 25000); // 25s keepalive
        }

        stopHeartbeat() {
            if (this.heartbeatTimer) {
                clearInterval(this.heartbeatTimer);
                this.heartbeatTimer = null;
            }
        }

        scheduleReconnect() {
            if (this.reconnectTimer) return;
            this.reconnectTimer = setTimeout(() => {
                this.reconnectTimer = null;
                this.connect();
            }, 4000);
        }

        reconnect() {
            if (this.ws) {
                try { this.ws.close(); } catch(e) {}
                this.ws = null;
            }
            this.connect();
        }

        handleMessage(msg) {
            // Manejo de eventos broadcast
            if (msg.event === 'broadcast' && msg.payload) {
                const subEvent = msg.payload.event;
                const data = msg.payload.payload || msg.payload.data;

                if (subEvent === 'remote_command' && this.onCommandCallback) {
                    this.onCommandCallback(data);
                } else if (subEvent === 'live_status' && this.onStatusCallback) {
                    this.onStatusCallback(data);
                }
            }
        }

        /**
         * Envía un comando desde el Celular al Computador del Templo
         */
        sendCommand(commandPayload) {
            if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
                console.warn('[SUPABASE_REALTIME] No conectado, reintentando...');
                this.connect();
                return false;
            }

            const ref = String(this.refCounter++);
            const broadcastMsg = {
                topic: 'realtime:' + this.channelName,
                event: 'broadcast',
                payload: {
                    type: 'broadcast',
                    event: 'remote_command',
                    payload: commandPayload
                },
                ref: ref
            };

            this.ws.send(JSON.stringify(broadcastMsg));
            return true;
        }

        /**
         * Envía el estado de proyección actual desde el Computador del Templo al Celular
         */
        sendStatus(statusPayload) {
            if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return false;

            const ref = String(this.refCounter++);
            const broadcastMsg = {
                topic: 'realtime:' + this.channelName,
                event: 'broadcast',
                payload: {
                    type: 'broadcast',
                    event: 'live_status',
                    payload: statusPayload
                },
                ref: ref
            };

            this.ws.send(JSON.stringify(broadcastMsg));
            return true;
        }

        disconnect() {
            this.stopHeartbeat();
            if (this.reconnectTimer) {
                clearTimeout(this.reconnectTimer);
                this.reconnectTimer = null;
            }
            if (this.ws) {
                try { this.ws.close(); } catch(e) {}
                this.ws = null;
            }
            this.isConnected = false;
            this.isConnecting = false;
        }
    }

    window.SupabaseRealtimeRemote = SupabaseRealtimeRemote;
})(typeof window !== 'undefined' ? window : this);
