# DE32_megabox_L

이 레포지토리는 https://github.com/DE32megabox/airflow 레포지토리의 하위속성 패키지입니다.
https://github.com/DE32megabox/airflow 레포지토리의 Extract, Transform, Load 과정중 Load의 역할을 담당하고 있습니다.

## 설치

이 레포지토리는 다음과 같이 설치할 수 있습니다.
```
pip install git+https://github.com/DE32megabox/load.git
```
다만 본 레포지토리는 https://github.com/DE32megabox/airflow 레포지토리의 하위 패키지이므로
https://github.com/DE32megabox/airflow 설치과정 없이 본 패키지 단독 설치는 의미가 없습니다.

## 환경설정

개발을 위한 설정은 다음 코드를 참조하여 주십시오.

```
pip install git+https://github.com/DE32megabox/load.git@dev/d1.0.0
```
```
pdm venv create
source .venv/bin/activate
```

## 동작 내용
본 패키지 함수 호출시 
- Transform 처리가 완료된 파일을 import한 다음
- ~/data/DE32_megabox/load 디렉토리 안에 데이터를 일자별로 export합니다.
- 모든 데이터는 parquet 파일로 저장되며 일자별로 디렉토리가 자동생성되어집니다.
- https://github.com/DE32megabox/ 일련과정의 데이터는 2021년의 데이터를 기준으로 작업됩니다.

