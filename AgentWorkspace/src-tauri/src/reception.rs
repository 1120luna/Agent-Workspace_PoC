use chrono::Utc;
use uuid::Uuid;

use crate::types::{CoreRequest, CoreResponse, FeInputPayload};

/// [R-001 / R-100] 입력 수신 및 유효성 검증 엔진
pub struct ReceptionEngine;

impl ReceptionEngine {
    /// 프론트엔드로부터 전달된 입력을 수신하여 검증(R-100)하고
    /// D-002 원문 보존 원칙에 따라 구조화된 CoreRequest를 생성한다.
    pub fn process_input(payload: FeInputPayload) -> Result<CoreRequest, CoreResponse> {
        // [R-100] 빈 값 및 공백 문자열 검증
        // 공백 제거 후 길이가 0이면 즉시 거부 (O-102 상태 코드 반환)
        if payload.prompt.trim().is_empty() {
            return Err(CoreResponse::rejected(
                None,
                "R-100: 입력값이 비어있거나 공백 문자열입니다. 유효한 프롬프트를 입력해 주세요.",
            ));
        }

        // 과도한 페이로드 길이 공격 방지 (예: 100,000자 제한)
        const MAX_PROMPT_LENGTH: usize = 100_000;
        if payload.prompt.len() > MAX_PROMPT_LENGTH {
            return Err(CoreResponse::rejected(
                None,
                format!(
                    "R-100: 프롬프트 길이가 최대 허용치({} 자)를 초과했습니다. (현재: {} 자)",
                    MAX_PROMPT_LENGTH,
                    payload.prompt.len()
                ),
            ));
        }

        // [UUID v4] 요청 식별자 생성
        let request_id = Uuid::new_v4().to_string();

        // 세션 ID가 제공되지 않은 경우 새로운 기본 세션 식별자 할당
        let session_id = payload
            .session_id
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let metadata = payload.metadata.unwrap_or_else(|| serde_json::json!({}));

        // [D-002] 줄바꿈, 탭, 선행/후행 공백 등 어떠한 변형도 가하지 않고 원문 그대로 보존
        let raw_prompt = payload.prompt;

        let request = CoreRequest {
            request_id,
            raw_prompt,
            session_id,
            status: "RECEIVED".to_string(),
            created_at: Utc::now().to_rfc3339(),
            metadata,
        };

        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ResponseCode;

    #[test]
    fn test_r100_empty_input_rejected() {
        let empty_payload = FeInputPayload {
            prompt: "".to_string(),
            session_id: None,
            metadata: None,
        };

        let result = ReceptionEngine::process_input(empty_payload);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ResponseCode::InputRejected);
        assert!(err.message.contains("R-100"));
    }

    #[test]
    fn test_r100_whitespace_only_rejected() {
        let whitespace_payload = FeInputPayload {
            prompt: "   \n\t  \r\n   ".to_string(),
            session_id: None,
            metadata: None,
        };

        let result = ReceptionEngine::process_input(whitespace_payload);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ResponseCode::InputRejected);
    }

    #[test]
    fn test_d002_raw_prompt_preserved_exactly() {
        let raw_text = "  Line 1: Special Characters !@#$%^&*()\nLine 2: Tab\tIndented\r\nLine 3: Trailing Space   ";
        let valid_payload = FeInputPayload {
            prompt: raw_text.to_string(),
            session_id: Some("session-123".to_string()),
            metadata: Some(serde_json::json!({"client": "tauri-ui"})),
        };

        let result = ReceptionEngine::process_input(valid_payload);
        assert!(result.is_ok());
        let core_req = result.unwrap();

        // D-002: 원문 일치 검증
        assert_eq!(core_req.raw_prompt, raw_text);
        assert_eq!(core_req.session_id, "session-123");
        assert_eq!(core_req.status, "RECEIVED");
        assert!(!core_req.request_id.is_empty());
        // UUID v4 유효성 검사
        assert!(Uuid::parse_str(&core_req.request_id).is_ok());
    }
}

