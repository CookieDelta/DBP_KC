package job.examen

import utils.TestInit
import job.examen.examen._


class TestE2 extends TestInit {

  import spark.implicits._

  "Caso 1" should "devolver un DF vacío al ingresar un DF vacío" in {

    val dfempty = Seq.empty[Int].toDF("numero")

    val salida = ejercicio2(dfempty)

    assert(salida.isEmpty)
  }
  "Caso 2" should "devolver una columna con el resultado par o impar según el caso" in {

    val numeros = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).toDF("numero")
    val salidaesperada = Seq(
      (1, "Impar"), (2, "Par"),
      (3, "Impar"), (4, "Par"),
      (5, "Impar"), (6, "Par"),
      (7, "Impar"), (8, "Par"),
      (9, "Impar"), (10, "Par")
    ).toDF("numero", "Par-impar")

    val resultado = ejercicio2(numeros)

    checkDf(salidaesperada, resultado)
  }
}