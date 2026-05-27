import Foundation
import OpenIslandCore
import os

@MainActor
final class NtfyRemoteNotifier {
    private nonisolated(unsafe) static let logger = Logger(subsystem: "app.openisland", category: "NtfyRemoteNotifier")

    struct Config {
        var server: String
        var topic: String

        var isConfigured: Bool {
            !server.isEmpty && !topic.isEmpty
        }

        var responseTopic: String { "\(topic)-response" }
    }

    enum PendingRequestKind {
        case permission(sessionID: String)
        case question(sessionID: String)
    }

    private struct PendingRequest {
        let kind: PendingRequestKind
        let createdAt: Date
    }

    private var pendingRequests: [String: PendingRequest] = [:]
    private var connectionTask: Task<Void, Never>?

    var config: Config {
        Config(
            server: UserDefaults.standard.string(forKey: "ntfy.server") ?? "",
            topic: UserDefaults.standard.string(forKey: "ntfy.topic") ?? ""
        )
    }

    var hasPendingRequests: Bool { !pendingRequests.isEmpty }

    var onPermissionResponse: ((String, Bool) -> Void)?
    var onQuestionResponse: ((String, String) -> Void)?

    // MARK: - Lifecycle

    func start() {
        guard connectionTask == nil else { return }
        guard config.isConfigured else {
            Self.logger.info("start: ntfy not configured, skipping WebSocket connection")
            return
        }
        Self.logger.info("start: launching persistent WebSocket connection")
        connectionTask = Task { [weak self] in
            await self?.runWebSocketLoop()
        }
    }

    func stop() {
        Self.logger.info("stop: tearing down WebSocket, clearing \(self.pendingRequests.count) pending requests")
        connectionTask?.cancel()
        connectionTask = nil
        pendingRequests.removeAll()
    }

    // MARK: - Send Notifications

    func sendPermissionNotification(sessionID: String, session: AgentSession) {
        guard config.isConfigured,
              let request = session.permissionRequest else {
            Self.logger.warning("sendPermissionNotification skipped: configured=\(self.config.isConfigured), hasPermissionRequest=\(session.permissionRequest != nil)")
            return
        }

        let requestID = UUID().uuidString
        pendingRequests[requestID] = PendingRequest(
            kind: .permission(sessionID: sessionID),
            createdAt: Date()
        )

        Self.logger.info("Sending permission notification: sessionID=\(sessionID), requestID=\(requestID), tool=\(request.toolName ?? "nil"), pending=\(self.pendingRequests.count)")

        let title = "Open Island: \(session.title) · \(request.toolName ?? "Permission")"
        let message = request.summary.isEmpty ? request.title : request.summary

        let responseURL = "\(config.server)/\(config.responseTopic)"
        let actions: [[String: Any]] = [
            [
                "action": "http",
                "label": "Approve",
                "url": responseURL,
                "method": "POST",
                "body": "{\"requestId\":\"\(requestID)\",\"approved\":true}",
                "clear": true,
            ],
            [
                "action": "http",
                "label": "Deny",
                "url": responseURL,
                "method": "POST",
                "body": "{\"requestId\":\"\(requestID)\",\"approved\":false}",
                "clear": true,
            ],
        ]

        let body: [String: Any] = [
            "topic": config.topic,
            "title": title,
            "message": String(message.prefix(1000)),
            "actions": actions,
        ]

        Task {
            await postNotification(body: body)
        }
    }

    func sendQuestionNotification(sessionID: String, session: AgentSession) {
        guard config.isConfigured,
              let prompt = session.questionPrompt else {
            Self.logger.warning("sendQuestionNotification skipped: configured=\(self.config.isConfigured), hasQuestionPrompt=\(session.questionPrompt != nil)")
            return
        }

        let requestID = UUID().uuidString
        pendingRequests[requestID] = PendingRequest(
            kind: .question(sessionID: sessionID),
            createdAt: Date()
        )

        Self.logger.info("Sending question notification: sessionID=\(sessionID), requestID=\(requestID), pending=\(self.pendingRequests.count)")

        let title = "Open Island: \(session.title) · Question"
        let questionText = prompt.questions.first?.question ?? prompt.title
        let responseURL = "\(config.server)/\(config.responseTopic)"

        var actions: [[String: Any]] = []
        let options = prompt.questions.first?.options ?? []
        for option in options.prefix(3) {
            actions.append([
                "action": "http",
                "label": option.label,
                "url": responseURL,
                "method": "POST",
                "body": "{\"requestId\":\"\(requestID)\",\"answer\":\"\(escapeJSON(option.label))\"}",
                "clear": true,
            ])
        }

        let body: [String: Any] = [
            "topic": config.topic,
            "title": title,
            "message": String(questionText.prefix(1000)),
            "actions": actions,
        ]

        Task {
            await postNotification(body: body)
        }
    }

    // MARK: - Private

    private func postNotification(body: [String: Any]) async {
        guard let url = URL(string: config.server) else {
            Self.logger.error("postNotification: invalid server URL '\(self.config.server)'")
            return
        }
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try? JSONSerialization.data(withJSONObject: body)

        do {
            let (_, response) = try await URLSession.shared.data(for: request)
            let statusCode = (response as? HTTPURLResponse)?.statusCode ?? -1
            Self.logger.info("postNotification: HTTP \(statusCode) to \(url.absoluteString)")
        } catch {
            Self.logger.error("postNotification failed: \(error.localizedDescription)")
        }
    }

    private nonisolated func runWebSocketLoop() async {
        let config = await self.config
        let wsURL = Self.buildWebSocketURL(config: config)
        guard let wsURL else {
            Self.logger.error("runWebSocketLoop: invalid WebSocket URL for server '\(config.server)'")
            return
        }

        Self.logger.info("runWebSocketLoop: target \(wsURL.absoluteString)")

        var consecutiveFailures = 0

        while !Task.isCancelled {
            let wsTask = URLSession.shared.webSocketTask(with: wsURL)
            wsTask.resume()

            Self.logger.info("runWebSocketLoop: WebSocket connected (failures=\(consecutiveFailures))")

            do {
                consecutiveFailures = 0
                while !Task.isCancelled {
                    let message = try await wsTask.receive()
                    await handleIncomingMessage(message)
                }
            } catch {
                if !Task.isCancelled {
                    Self.logger.error("runWebSocketLoop: WebSocket error: \(error.localizedDescription)")
                }
            }

            wsTask.cancel(with: .goingAway, reason: nil)

            guard !Task.isCancelled else { break }

            let delay = Self.backoffDelay(failures: consecutiveFailures)
            consecutiveFailures += 1
            Self.logger.info("runWebSocketLoop: reconnecting in \(String(format: "%.1f", delay))s (attempt \(consecutiveFailures))")
            try? await Task.sleep(for: .seconds(delay))

            await pruneExpiredRequests()
        }

        Self.logger.info("runWebSocketLoop: exiting (cancelled)")
    }

    private func handleIncomingMessage(_ message: URLSessionWebSocketTask.Message) {
        let text: String
        switch message {
        case .string(let s):
            text = s
        case .data(let d):
            guard let s = String(data: d, encoding: .utf8) else { return }
            text = s
        @unknown default:
            return
        }

        Self.logger.debug("handleIncomingMessage: \(String(text.prefix(200)))")

        guard let data = text.data(using: .utf8),
              let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            return
        }

        guard let messageStr = json["message"] as? String else {
            Self.logger.debug("handleIncomingMessage: no 'message' field (event=\(json["event"] as? String ?? "unknown"))")
            return
        }

        guard let messageData = messageStr.data(using: .utf8),
              let responsePayload = try? JSONSerialization.jsonObject(with: messageData) as? [String: Any] else {
            Self.logger.warning("handleIncomingMessage: 'message' not valid JSON: \(String(messageStr.prefix(100)))")
            return
        }

        guard let requestID = responsePayload["requestId"] as? String else {
            Self.logger.debug("handleIncomingMessage: no 'requestId' in payload")
            return
        }

        guard let pending = pendingRequests.removeValue(forKey: requestID) else {
            Self.logger.debug("handleIncomingMessage: unknown requestID \(requestID), ignoring")
            return
        }

        Self.logger.info("handleIncomingMessage: matched requestID=\(requestID)")

        switch pending.kind {
        case .permission(let sessionID):
            if let approved = responsePayload["approved"] as? Bool {
                Self.logger.info("handleIncomingMessage: onPermissionResponse(sessionID=\(sessionID), approved=\(approved))")
                onPermissionResponse?(sessionID, approved)
            } else {
                Self.logger.warning("handleIncomingMessage: permission response missing 'approved' field")
            }
        case .question(let sessionID):
            if let answer = responsePayload["answer"] as? String {
                Self.logger.info("handleIncomingMessage: onQuestionResponse(sessionID=\(sessionID), answer=\(answer))")
                onQuestionResponse?(sessionID, answer)
            } else {
                Self.logger.warning("handleIncomingMessage: question response missing 'answer' field")
            }
        }
    }

    private func pruneExpiredRequests() {
        let cutoff = Date().addingTimeInterval(-600)
        let expired = pendingRequests.filter { $0.value.createdAt <= cutoff }
        for key in expired.keys {
            pendingRequests.removeValue(forKey: key)
        }
        if !expired.isEmpty {
            Self.logger.info("pruneExpiredRequests: removed \(expired.count) stale entries")
        }
    }

    private nonisolated static func buildWebSocketURL(config: Config) -> URL? {
        var urlString = config.server
        if urlString.hasPrefix("https://") {
            urlString = "wss://" + urlString.dropFirst(8)
        } else if urlString.hasPrefix("http://") {
            urlString = "ws://" + urlString.dropFirst(7)
        }
        urlString += "/\(config.responseTopic)/ws"
        return URL(string: urlString)
    }

    private nonisolated static func backoffDelay(failures: Int) -> TimeInterval {
        let base: TimeInterval = 5.0
        let maxDelay: TimeInterval = 120.0
        let jitter = Double.random(in: 0..<1.0)
        return min(base * pow(2.0, Double(failures)) + jitter, maxDelay)
    }

    private func escapeJSON(_ string: String) -> String {
        string
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\"", with: "\\\"")
            .replacingOccurrences(of: "\n", with: "\\n")
    }
}
