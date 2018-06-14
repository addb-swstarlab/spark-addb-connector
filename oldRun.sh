BASE_DIR="/home/hwan" # FOR BASE_DIR metastone_db
ADDB_DIR="workspace/addb-SRConnector"
case $1 in
	"sparkall")
		sbt clean assembly
		cd $BASE_DIR
		echo "Running on $(pwd)"
		spark-shell --jars=$ADDB_DIR/target/scala-2.11/addb-srconnector-assembly-1.0.jar
		;;
	"sqlall")
		sbt clean assembly
		cd $BASE_DIR
		echo "Running on $(pwd)"
		spark-sql --jars=$ADDB_DIR/target/scala-2.11/addb-srconnector-assembly-1.0.jar
		;;
	"compile")
		sbt clean assembly
		;;
	"spark")
		cd $BASE_DIR
		echo "Running on $(pwd)"
		spark-shell --jars=$ADDB_DIR/target/scala-2.11/addb-srconnector-assembly-1.0.jar
		;;
	"sql")
		cd $BASE_DIR
		echo "Running on $(pwd)"
		spark-sql --jars=$ADDB_DIR/target/scala-2.11/addb-srconnector-assembly-1.0.jar
		;;
	*)
		echo "[Possible command:]"
		echo "sparkall	//compile + run Spark"
		echo "sqlall		//compile + run SparkSQL"
		echo "compile		//compile only by sbt"
		echo "spark		//run Spark only"
		echo "sql		//run SparkSQL only"

		exit -1
		;;
esac
