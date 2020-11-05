library(dplyr)
library(sparklyr)
library(future)
SparkR::sparkR.session()
sc <- spark_connect(method = "databricks")

stream_generate_test(iterations = 1, path = "/dbfs/tmp/stream_source")

# Note that any spark streaming will apply dbfs:// before /tmp, which is why the path for the test
# generator and the path for the streaming operations are different.
stream_reader <- stream_read_csv(sc, "/tmp/stream_source")
stream_writer <- stream_write_memory(stream_reader, mode = "append", checkpoint = "/tmp/checkpoints", name = "XXXX1")

invisible(future(stream_generate_test(iterations = 100, interval = 0.5, path ="/dbfs/tmp/stream_source")))
stream_view(stream_writer)

sdf_XXXX <- dplyr::tbl(sc, "XXXX1") 

sdf_dim(sdf_XXXX)

stream_stop(stream_writer)

dplyr::db_drop_table(sc, "XXXX1")

if(dir.exists("/dbfs/tmp/stream_source"))
  unlink("/dbfs/tmp/stream_source", recursive = TRUE)

if(dir.exists("/dbfs/tmp/checkpoints"))
  unlink("/dbfs/tmp/checkpoints", recursive = TRUE)

spark_disconnect(sc)