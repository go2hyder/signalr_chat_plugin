import 'dart:io';

import 'package:http/http.dart';
import 'package:http/io_client.dart';

/// Returns an [IOClient] that accepts all TLS certificates,
/// including self-signed and expired ones.
BaseClient? createBypassSslHttpClient() {
  final httpClient = HttpClient()
    ..badCertificateCallback = (_, __, ___) => true;
  return IOClient(httpClient);
}
