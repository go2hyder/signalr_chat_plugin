import 'package:signalr_core/signalr_core.dart';

class SignalRConnectionOptions {
  final String serverUrl;
  final String? accessToken;
  final Duration reconnectInterval;
  final int maxRetryAttempts;
  final bool autoReconnect;
  final Function(String)? onError;
  final bool useSecureConnection;
  final HttpTransportType transport;
  final bool skipNegotiation;

  /// When `true`, accepts invalid or self-signed TLS certificates.
  /// Only effective on mobile/desktop (IO platforms); browsers enforce
  /// their own certificate policies and ignore this flag.
  /// **WARNING:** Do not enable in production.
  final bool bypassSslCertificateValidation;

  SignalRConnectionOptions({
    required this.serverUrl,
    this.accessToken,
    this.reconnectInterval = const Duration(seconds: 5),
    this.maxRetryAttempts = 5,
    this.autoReconnect = true,
    this.onError,
    this.useSecureConnection = true,
    this.transport = HttpTransportType.webSockets,
    this.skipNegotiation = false,
    this.bypassSslCertificateValidation = false,
  });
}
