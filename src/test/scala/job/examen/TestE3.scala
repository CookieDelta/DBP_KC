package job.examen

import utils.TestInit
import job.examen.examen._

case class EstudianteTest(id: Int, nombre: String)
case class Calificaciones(id_estudiante: Int, asignatura: String, calificacion: Int)
case class Resultado(id_estudiante: Int, nombre: String, promedio: Option[Double])

class TestE3 extends TestInit {

  import spark.implicits._

  "E3 Caso 1" should "retorna DF vacío" in {
    val estudiantesdf = Seq.empty[EstudianteTest].toDF()
    val calificacionesdf = Seq.empty[Calificaciones].toDF()

    val resultado = ejercicio3(estudiantesdf, calificacionesdf)

    assert(resultado.isEmpty)
  }

  "E3 Caso 2" should "Retorna DF's unidos con el promedio por estudiante" in {
    val estudiantesdf2 = Seq(
      EstudianteTest(1, "Carlos"),
      EstudianteTest(2, "Ana"),
      EstudianteTest(3, "Luis"),
      EstudianteTest(4, "María")
    ).toDF()

    val calificacionesdf2 = Seq(
      Calificaciones(1, "Matemáticas", 94),
      Calificaciones(1, "Historia", 88),
      Calificaciones(1, "Ciencias", 92),
      Calificaciones(2, "Matemáticas", 85),
      Calificaciones(2, "Historia", 78),
      Calificaciones(2, "Ciencias", 80),
      Calificaciones(3, "Matemáticas", 75),
      Calificaciones(3, "Historia", 65),
      Calificaciones(3, "Ciencias", 70),
      Calificaciones(4, "Matemáticas", 95),
      Calificaciones(4, "Historia", 90),
      Calificaciones(4, "Ciencias", 98)
    ).toDF()

    val resultadoEsperado = Seq(
      Resultado(1, "Carlos", Some(91.33)),
      Resultado(2, "Ana", Some(81.0)),
      Resultado(3, "Luis", Some(70.0)),
      Resultado(4, "María", Some(94.33))
    ).toDF()

    val resultado = ejercicio3(estudiantesdf2, calificacionesdf2)

    checkDf(resultado, resultadoEsperado)
  }
}
