package job.examen

import utils.TestInit
import job.examen.examen._
import org.apache.spark.rdd.RDD

class TestE4 extends TestInit {

  "E4 Caso 1" should "Debería devolver un RDD vacío si la lista está vacía" in {
    // Lista vacía
    val palabras = List.empty[String]

    val resultado: RDD[(String, Int)] = ejercicio4(palabras)


    assert(resultado.isEmpty())
  }

  "E4 Caso 2" should "Debería contar correctamente las ocurrencias de cada palabra" in {

    val palabras = List("manzana", "banana", "manzana", "pera", "manzana", "pera", "pera")

    val resultadoEsperado = Map(
      "manzana" -> 3,
      "banana" -> 1,
      "pera" -> 3
    )

    val resultado: RDD[(String, Int)] = ejercicio4(palabras)

    val resultadoComoMapa = resultado.collect().toMap

    assert(resultadoComoMapa == resultadoEsperado)
  }
}

