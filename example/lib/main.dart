import 'package:flutter/material.dart';
import 'package:signalr_chat_plugin/signalr_plugin.dart';
import 'package:signalr_chat_plugin/user_room_connection.dart';
import 'dart:developer' as developer;
import 'package:signalr_core/signalr_core.dart';

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(const ChatApp());
}

class ChatApp extends StatelessWidget {
  const ChatApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        primarySwatch: Colors.blue,
        useMaterial3: true,
      ),
      home: const StartScreen(),
    );
  }
}

class StartScreen extends StatelessWidget {
  const StartScreen({super.key});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: Center(
        child: ElevatedButton(
          onPressed: () {
            Navigator.of(context).push(
              MaterialPageRoute(
                builder: (context) =>
                    const ChatScreen(room: "UUUUU", userName: "Vishnu"),
              ),
            );
          },
          style: ElevatedButton.styleFrom(
            padding: const EdgeInsets.symmetric(horizontal: 32, vertical: 16),
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(30),
            ),
          ),
          child: const Text(
            'Join Room',
            style: TextStyle(fontSize: 18),
          ),
        ),
      ),
    );
  }
}

class ChatScreen extends StatefulWidget {
  final String room;
  final String userName;
  const ChatScreen({super.key, required this.room, required this.userName});

  @override
  State<ChatScreen> createState() => _ChatScreenState();
}

class _ChatScreenState extends State<ChatScreen> {
  final TextEditingController _messageController = TextEditingController();
  final ScrollController _scrollController = ScrollController();
  final SignalRChatPlugin _chatPlugin = SignalRChatPlugin();
  final List<ChatMessage> _messages = [];
  final Set<String> _processedMessageIds = {};
  late String _username;
  late String _room;
  ConnectionStatus _connectionState = ConnectionStatus.connecting;
  bool _isInitialized = false;

  @override
  void initState() {
    super.initState();
    _username = widget.userName;
    _room = widget.room;
    _initializeChat();
  }

  Future<void> _initializeChat() async {
    try {
      developer.log('Initializing SignalR connection...');

      // First set up all the listeners before initializing
      _setupListeners();

      // Initialize SignalR with configuration
      await _chatPlugin.initSignalR(
        SignalRConnectionOptions(
          serverUrl: 'https://your-signalR-endpoint',
          reconnectInterval: const Duration(seconds: 3),
          maxRetryAttempts: 5,
          autoReconnect: true,
          onError: (error) {
            developer.log('SignalR error: $error');
            _handleError(error);
          },
          transport:
              HttpTransportType.webSockets, // Explicitly set transport type
          skipNegotiation: true, // Explicitly set skipNegotiation
        ),
      );

      developer.log('SignalR initialized successfully');

      // Add a small delay to ensure connection is fully established
      await Future.delayed(const Duration(milliseconds: 500));

      // Now join the room
      developer
          .log('Attempting to join room: $_room with username: $_username');

      await _chatPlugin.joinRoom(UserRoomConnection(
        user: _username,
        room: _room,
      ));

      developer.log('Successfully joined room');

      // Mark as initialized
      setState(() {
        _isInitialized = true;
        _connectionState = ConnectionStatus.connected;
      });

      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('Connected to chat'),
            backgroundColor: Colors.green,
            duration: Duration(seconds: 2),
          ),
        );
      }
    } catch (e, stackTrace) {
      developer.log('Error initializing chat: $e');
      developer.log('Stack trace: $stackTrace');

      setState(() {
        _connectionState = ConnectionStatus.disconnected;
      });

      _handleError('Failed to initialize chat: $e');

      // Retry connection after a delay
      if (mounted) {
        Future.delayed(const Duration(seconds: 3), () {
          if (mounted && !_isInitialized) {
            _initializeChat();
          }
        });
      }
    }
  }

  void _setupListeners() {
    // Listen to messages
    _chatPlugin.messagesStream.listen(
      _handleNewMessage,
      onError: (error) {
        developer.log('Message stream error: $error');
        _handleError(error.toString());
      },
    );

    // Listen to connected users
    _chatPlugin.connectedUsersStream.listen(
      (users) {
        developer.log('Connected users updated: $users');
        // Handle connected users if needed
      },
      onError: (error) {
        developer.log('Connected users stream error: $error');
        _handleError(error.toString());
      },
    );

    // Listen to connection state changes
    _chatPlugin.connectionStateStream.listen(
      (state) {
        developer.log('Connection state changed to: $state');
        _handleConnectionState(state);
      },
      onError: (error) {
        developer.log('Connection state stream error: $error');
        _handleError(error.toString());
      },
    );

    // Listen to errors
    _chatPlugin.errorStream.listen(
      (error) {
        developer.log('Error stream received: $error');
        _handleError(error);
      },
      onError: (error) {
        developer.log('Error stream error: $error');
        _handleError(error.toString());
      },
    );
  }

  void _handleNewMessage(ChatMessage message) {
    developer
        .log('Received message from ${message.sender}: ${message.content}');

    // Skip if we've already processed this message by ID
    if (message.messageId != null &&
        _processedMessageIds.contains(message.messageId)) {
      developer.log('Skipping duplicate message by ID: ${message.messageId}');
      return;
    }

    // Add message ID to processed set if it has one
    if (message.messageId != null) {
      _processedMessageIds.add(message.messageId!);
    }

    setState(() {
      // If this is our own message, check if we already have a similar message in the list
      if (message.sender == _username) {
        // Look for an existing message with the same content from the same sender
        // that was sent within the last few seconds
        final now = DateTime.now();
        final existingMsgIndex = _messages.indexWhere((m) =>
            m.sender == _username &&
            m.content == message.content &&
            now.difference(m.timestamp).inSeconds < 5);

        if (existingMsgIndex != -1) {
          // Update the existing message status
          _messages[existingMsgIndex] = message;
          developer.log('Updated existing message instead of adding duplicate');
          return;
        }
      }

      // Add new message
      _messages.insert(0, message);
    });
    _scrollToBottom();
  }

  void _handleConnectionState(ConnectionStatus state) {
    developer.log('Handling connection state: $state');
    if (!mounted) return;

    setState(() {
      _connectionState = state;
    });

    // Don't show snackbar for initial connecting state
    if (state == ConnectionStatus.connecting && !_isInitialized) return;

    String message;
    Color backgroundColor;

    switch (state) {
      case ConnectionStatus.connected:
        message = 'Connected to chat';
        backgroundColor = Colors.green;
        break;
      case ConnectionStatus.connecting:
        message = 'Connecting to chat...';
        backgroundColor = Colors.orange;
        break;
      case ConnectionStatus.reconnecting:
        message = 'Reconnecting...';
        backgroundColor = Colors.orange;
        break;
      case ConnectionStatus.disconnected:
        message = 'Disconnected from chat';
        backgroundColor = Colors.red;
        break;
    }

    if (mounted) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text(message),
          backgroundColor: backgroundColor,
          duration: const Duration(seconds: 2),
        ),
      );
    }
  }

  void _handleError(String error) {
    if (!mounted) return;

    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text('Error: $error'),
        backgroundColor: Colors.red,
        duration: const Duration(seconds: 3),
      ),
    );
  }

  Future<void> _sendMessage() async {
    final message = _messageController.text.trim();
    if (message.isEmpty) return;

    // Check if initialized
    if (!_isInitialized) {
      _handleError('Chat not initialized. Please wait...');
      return;
    }

    // Check connection state
    if (_connectionState != ConnectionStatus.connected) {
      _handleError('Not connected to server');
      return;
    }

    try {
      developer.log('Sending message: $message');
      _messageController.clear();

      // Create a temporary message to show in UI while sending
      final messageId = DateTime.now().millisecondsSinceEpoch.toString();
      final tempMessage = ChatMessage(
        sender: _username,
        content: message,
        timestamp: DateTime.now(),
        messageId: messageId,
        status: MessageStatus.sending,
      );

      // Add the temporary message to UI
      setState(() {
        _messages.insert(0, tempMessage);
        _processedMessageIds.add(messageId);
      });
      _scrollToBottom();

      // Send the message
      await _chatPlugin.sendMessage(message , false);
      developer.log('Message sent successfully');
    } catch (e, stackTrace) {
      developer.log('Error sending message: $e');
      developer.log('Stack trace: $stackTrace');

      // Update the message status to failed
      setState(() {
        final index = _messages
            .indexWhere((m) => m.content == message && m.sender == _username);
        if (index != -1) {
          _messages[index] = ChatMessage(
            sender: _username,
            content: message,
            timestamp: _messages[index].timestamp,
            messageId: _messages[index].messageId,
            status: MessageStatus.failed,
          );
        }
      });

      _handleError('Failed to send message');
    }
  }

  void _scrollToBottom() {
    if (_scrollController.hasClients) {
      Future.delayed(const Duration(milliseconds: 100), () {
        if (_scrollController.hasClients) {
          _scrollController.animateTo(
            0,
            duration: const Duration(milliseconds: 300),
            curve: Curves.easeOut,
          );
        }
      });
    }
  }

  Widget _buildConnectionStatus() {
    Color color;
    IconData icon;
    String text;

    switch (_connectionState) {
      case ConnectionStatus.connected:
        color = Colors.green;
        icon = Icons.cloud_done;
        text = 'Connected';
        break;
      case ConnectionStatus.connecting:
        color = Colors.orange;
        icon = Icons.cloud_upload;
        text = 'Connecting';
        break;
      case ConnectionStatus.reconnecting:
        color = Colors.orange;
        icon = Icons.cloud_upload;
        text = 'Reconnecting';
        break;
      case ConnectionStatus.disconnected:
        color = Colors.red;
        icon = Icons.cloud_off;
        text = 'Disconnected';
        break;
    }

    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
      margin: const EdgeInsets.only(right: 8),
      decoration: BoxDecoration(
        color: color.withValues(alpha: 0.1),
        borderRadius: BorderRadius.circular(16),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(icon, size: 16, color: color),
          const SizedBox(width: 4),
          Text(
            text,
            style: TextStyle(color: color, fontSize: 12),
          ),
        ],
      ),
    );
  }

  Widget _buildMessageItem(ChatMessage message) {
    final isMyMessage = message.sender == _username;

    return Align(
      alignment: isMyMessage ? Alignment.centerRight : Alignment.centerLeft,
      child: Container(
        margin: const EdgeInsets.symmetric(vertical: 4, horizontal: 8),
        padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
        constraints: BoxConstraints(
          maxWidth: MediaQuery.of(context).size.width * 0.7,
        ),
        decoration: BoxDecoration(
          color: isMyMessage ? Colors.blue : Colors.grey[300],
          borderRadius: BorderRadius.only(
            topLeft: const Radius.circular(12),
            topRight: const Radius.circular(12),
            bottomLeft: isMyMessage ? const Radius.circular(12) : Radius.zero,
            bottomRight: isMyMessage ? Radius.zero : const Radius.circular(12),
          ),
        ),
        child: Column(
          crossAxisAlignment:
              isMyMessage ? CrossAxisAlignment.end : CrossAxisAlignment.start,
          children: [
            Text(
              message.sender,
              style: TextStyle(
                fontSize: 12,
                fontWeight: FontWeight.bold,
                color: isMyMessage ? Colors.white70 : Colors.black54,
              ),
            ),
            const SizedBox(height: 4),
            Text(
              message.content,
              style: TextStyle(
                color: isMyMessage ? Colors.white : Colors.black,
              ),
            ),
            const SizedBox(height: 4),
            Row(
              mainAxisSize: MainAxisSize.min,
              children: [
                Text(
                  _formatTime(message.timestamp),
                  style: TextStyle(
                    fontSize: 10,
                    color: isMyMessage ? Colors.white70 : Colors.black54,
                  ),
                ),
                if (isMyMessage) ...[
                  const SizedBox(width: 4),
                  _buildMessageStatus(message.status),
                ],
              ],
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildMessageStatus(MessageStatus status) {
    IconData icon;
    Color color;

    switch (status) {
      case MessageStatus.sending:
        icon = Icons.schedule;
        color = Colors.white70;
        break;
      case MessageStatus.delivered:
        icon = Icons.done_all;
        color = Colors.white;
        break;
      case MessageStatus.failed:
        icon = Icons.error_outline;
        color = Colors.red[300]!;
        break;
    }

    return Icon(icon, size: 14, color: color);
  }

  String _formatTime(DateTime time) {
    return '${time.hour.toString().padLeft(2, '0')}:${time.minute.toString().padLeft(2, '0')}';
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text('SignalR Chat'),
            Text(
              'Room: $_room',
              style: const TextStyle(fontSize: 12),
            ),
          ],
        ),
        actions: [_buildConnectionStatus()],
      ),
      body: Column(
        children: [
          Expanded(
            child: _messages.isEmpty
                ? Center(
                    child: Text(
                      _isInitialized
                          ? 'No messages yet. Start a conversation!'
                          : 'Connecting to chat...',
                      style: TextStyle(color: Colors.grey[600]),
                    ),
                  )
                : ListView.builder(
                    controller: _scrollController,
                    reverse: true,
                    itemCount: _messages.length,
                    itemBuilder: (context, index) =>
                        _buildMessageItem(_messages[index]),
                  ),
          ),
          Container(
            padding: const EdgeInsets.all(8.0),
            decoration: BoxDecoration(
              color: Colors.white,
              boxShadow: [
                BoxShadow(
                  color: Colors.black.withValues(alpha: 0.1),
                  blurRadius: 4,
                  offset: const Offset(0, -2),
                ),
              ],
            ),
            child: Row(
              children: [
                Expanded(
                  child: TextField(
                    controller: _messageController,
                    enabled: _isInitialized &&
                        _connectionState == ConnectionStatus.connected,
                    decoration: InputDecoration(
                      hintText: _isInitialized
                          ? 'Type a message...'
                          : 'Connecting...',
                      border: OutlineInputBorder(
                        borderRadius: BorderRadius.circular(24),
                      ),
                      contentPadding: const EdgeInsets.symmetric(
                        horizontal: 16,
                        vertical: 8,
                      ),
                    ),
                    onSubmitted: (_) => _sendMessage(),
                  ),
                ),
                const SizedBox(width: 8),
                FloatingActionButton(
                  onPressed: _isInitialized &&
                          _connectionState == ConnectionStatus.connected
                      ? _sendMessage
                      : null,
                  backgroundColor: _isInitialized &&
                          _connectionState == ConnectionStatus.connected
                      ? Theme.of(context).primaryColor
                      : Colors.grey,
                  child: const Icon(Icons.send),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  @override
  void dispose() {
    developer.log('Disposing chat screen');
    _messageController.dispose();
    _scrollController.dispose();
    _chatPlugin.disconnect();
    _processedMessageIds.clear();
    super.dispose();
  }
}
