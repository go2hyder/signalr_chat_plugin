import 'package:http/http.dart';

/// Stub for web platforms where `dart:io` is unavailable.
/// Returns `null` — browsers enforce their own certificate policies.
BaseClient? createBypassSslHttpClient() => null;
