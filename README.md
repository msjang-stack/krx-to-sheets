# KRX → Google Sheets (GitHub Actions)

**설정값 반영판**: 매일 **평일 오후 4시(KST)**에 실행되도록 `cron`이 이미 설정되어 있습니다.  
종목은 GitHub Secrets의 `TICKERS`에 `082270,358570,000250`로 넣어 사용하세요.

## 준비물(요약)
1. Google Cloud 서비스 계정 + JSON 키 파일
2. 스프레드시트 생성 후 서비스 계정 **편집자 공유**, 그리고 **스프레드시트 ID** 준비
3. 이 리포지토리를 GitHub에 올린 뒤 **Secrets** 추가
   - `GOOGLE_SERVICE_ACCOUNT_JSON` = JSON 내용 전체
   - `SPREADSHEET_ID` = 스프레드시트 ID
   - (옵션) `WORKSHEET_NAME` = 기본 daily_log
   - (권장) `TICKERS` = `082270,358570,000250`

## 실행 시간
- 평일 매일 **16:00 KST (UTC 07:00)**  
- `.github/workflows/daily.yml`의 `cron: "0 7 * * 1-5"`로 설정되어 있습니다.

## 수동 테스트
- GitHub → Actions 탭 → Run workflow

## 파일 구성
- `krx_daily_to_sheet.py` : 데이터 수집/기록 스크립트(환경변수 사용)
- `requirements.txt` : 의존성
- `.github/workflows/daily.yml` : GitHub Actions 워크플로우
