"""
Smart Factory Sensor 로그 생성기
- 해당 파이썬 파일은 장비로 이해 -> 장비가 신호/로그 발생 -> 감지 -> 데이터 파이프라인 전개 구조
- 로그 파일
    - sensor_json.log : JSONL 포멧
    - sensor_text.log : Text  포멧
    - 각 파일이 10MB 도달하면 로테이션 시도 -> xxx-1, xxx-2,... 파일 신규로 생성
    - 최대 유지 파일수는 5개 설정, 6개가 되면 가장 오래된 파일 1개를 삭제
"""