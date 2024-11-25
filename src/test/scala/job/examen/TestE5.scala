package job.examen

import utils.TestInit
import job.examen.examen._
import org.apache.spark.sql.DataFrame

class TestE5 extends TestInit {

  import spark.implicits._

  "E5 Caso 1" should "Devolver un DF vacío" in {

    val ventasVacias: DataFrame = Seq.empty[(Int, Int, Int, Double)]
      .toDF("id_venta", "id_producto", "cantidad", "precio_unitario")

    val resultado: DataFrame = ejercicio5(ventasVacias)(spark)

    assert(resultado.isEmpty)
  }

  "E5 Caso 2" should "Calcular el ingreso total por producto correctamente desde ventas.csv" in {

    val rutaCSV = "src/test/resources/ventas.csv"
    val ventas: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(rutaCSV)
      .toDF("id_venta", "id_producto", "cantidad", "precio_unitario")


    val resultado: DataFrame = ejercicio5(ventas)(spark)


    val esperado: DataFrame = Seq(
      (101, 46.5),  //solo datos ficticios para que corra el test y de error, entonces podré saber los valores
      (102, 100.0),
      (103, 50.0)
    ).toDF("id_producto", "ingreso_total")

    assert(resultado.collect().toSet == esperado.collect().toSet)
  }
}
