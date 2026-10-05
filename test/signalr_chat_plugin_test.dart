import 'package:flutter_test/flutter_test.dart';
import 'package:signalr_chat_plugin/signalr_chat_plugin_platform_interface.dart';
import 'package:signalr_chat_plugin/signalr_chat_plugin_method_channel.dart';
import 'package:plugin_platform_interface/plugin_platform_interface.dart';

class MockSignalrChatPluginPlatform
    with MockPlatformInterfaceMixin
    implements SignalrChatPluginPlatform {
  @override
  Future<String?> getPlatformVersion() => Future.value('42');
}

void main() {
  final SignalrChatPluginPlatform initialPlatform =
      SignalrChatPluginPlatform.instance;

  test('$MethodChannelSignalrChatPlugin is the default instance', () {
    expect(initialPlatform, isInstanceOf<MethodChannelSignalrChatPlugin>());
  });

  test('getPlatformVersion', () async {
    // SignalrChatPlugin signalrChatPlugin = SignalrChatPlugin();
    MockSignalrChatPluginPlatform fakePlatform =
        MockSignalrChatPluginPlatform();
    SignalrChatPluginPlatform.instance = fakePlatform;

    // expect(await signalrChatPlugin.getPlatformVersion(), '42');
  });
}
