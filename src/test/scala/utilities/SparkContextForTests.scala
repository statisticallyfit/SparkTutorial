//package utils;trait SparkContextForTests extends Suite with BeforeAndAfterAll {
//        var sc: SparkContext = _
//
//        override def beforeAll() {
//                sc = new SparkContext("local", "Testing", System.getenv("SPARK_HOME"))
//                }
//
//        override def afterAll() {
//                sc.stop()
//                }
//        }
