import 'dart:convert' show latin1, utf8;

// message.dart
class ChatMessage {
  final String sender;
  final String content;
  final DateTime timestamp;
  final String? messageId;
  final MessageStatus status;
  bool? hasFile;

  String getArabicFixedContent() {
    try {
      return utf8.decode(latin1.encode(content));
    } catch (e) {
      // If decoding fails, return the original
      return content;
    }
  }

  ChatMessage({
    required this.sender,
    required this.content,
    required this.timestamp,
    this.messageId,
    this.status = MessageStatus.sending,
    this.hasFile,
  });

  Map<String, dynamic> toJson() => {
    'sender': sender,
    'content': content,
    'timestamp': timestamp.toIso8601String(),
    'messageId': messageId,
    'status': status.toString(),
    'hasFile': hasFile,
  };

  factory ChatMessage.fromJson(Map<String, dynamic> json) {
    return ChatMessage(
      sender: json['sender'] as String,
      content: json['content'] as String,
      timestamp: DateTime.parse(json['timestamp'] as String),
      messageId: json['messageId'] as String?,
      hasFile: json['hasFile'] as bool?,
      status: MessageStatus.values.firstWhere(
            (e) => e.toString() == json['status'],
        orElse: () => MessageStatus.sending,
      ),
    );
  }
}

enum MessageStatus { sending, delivered, failed }

enum ConnectionStatus { connecting, connected, disconnected, reconnecting }
