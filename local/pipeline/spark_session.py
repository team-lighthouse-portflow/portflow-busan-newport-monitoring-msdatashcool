import os, glob

# ── Java 17 자동 탐색 (pyspark import 전에 반드시!) ──
def _find_jdk17():
    hits = []
    for base in [r"C:\Program Files\Eclipse Adoptium",
                 r"C:\Program Files\Java",
                 r"C:\Program Files\Microsoft"]:
        hits += glob.glob(os.path.join(base, "*jdk-17*"))
        hits += glob.glob(os.path.join(base, "*jdk17*"))
    return hits[0] if hits else None

JAVA_HOME = _find_jdk17()
assert JAVA_HOME, ("Java 17을 못 찾았습니다. "
                   "'winget install EclipseAdoptium.Temurin.17.JDK' 설치 후 다시 실행하세요.")

os.environ["JAVA_HOME"]   = JAVA_HOME
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = (
    os.path.join(JAVA_HOME, "bin") + os.pathsep +
    os.path.join(r"C:\hadoop", "bin") + os.pathsep +
    os.environ["PATH"]
)
print(f"[spark_session] 사용 중인 JAVA_HOME = {JAVA_HOME}")  # ← 17 경로인지 눈으로 확인

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

def get_spark(app_name="busan-port", extra_packages=None):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "4g")
    )
    # delta JAR + (있으면) 추가 패키지(JDBC 드라이버 등)를 함께 로드
    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()