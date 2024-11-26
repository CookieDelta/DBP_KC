package job.examen

import utils.TestInit
import job.examen.examen._

case class Estudiante(nombre:String, edad: Int, calificacion:Double)

class TestE1 extends TestInit {
  import spark.implicits._

  "Ejercicio 1- Case 1" should "Al no ingresar datos, debería devolver un DF vacío" in {
    val estudiantesDF = Seq.empty[Estudiante].toDF() //"si hay una lista vacía, conviertela a DF"
    val DFsalida = ejercicio1(estudiantesDF) //la salida de la función

    assert(DFsalida.isEmpty) //la salida debería ser un DF vacpio
  }

  "Ejercicio 1 - Case 2" should "Devolver nombres de los estudiantes con notas > 8, ordenados de forma descendente" in {

    val estudiantesData = Seq(
      ("Juan", 20, 9.5),
      ("María", 22, 7.8),
      ("Luis", 19, 8.2),
      ("Ana", 21, 9.1),
      ("Pedro", 23, 6.5),
      ("Claudia", 20, 9.8),
      ("Carlos", 24, 7.1),
      ("Sofía", 18, 8.9),
      ("Javier", 25, 5.4),
      ("Lucía", 20, 8.6),
      ("Fernando", 22, 7.3),
      ("Valeria", 19, 9.0),
      ("Andrea", 21, 6.9),
      ("Diego", 23, 8.0),
      ("Mónica", 20, 7.6),
      ("Emilio", 24, 8.3),
      ("Carmen", 19, 6.4),
      ("Raúl", 21, 8.8),
      ("Diana", 20, 7.0),
      ("Pablo", 22, 5.9),
      ("Elena", 23, 9.3),
      ("Miguel", 18, 6.8),
      ("Laura", 19, 7.7),
      ("Ricardo", 24, 8.4),
      ("Isabel", 21, 9.2),
      ("Gabriel", 20, 8.1),
      ("Sara", 22, 6.7),
      ("Hugo", 23, 9.6),
      ("Natalia", 18, 8.5),
      ("Adrián", 19, 7.4)
    ).toDF("nombre","edad","calificacion") //Se añadieron 30 estudiantes a Data, como tupla

    val salidaesperada = Seq(
      "Claudia",
      "Hugo",
      "Juan",
      "Elena",
      "Isabel",
      "Ana",
      "Valeria",
      "Sofía",
      "Raúl",
      "Lucía",
      "Natalia",
      "Ricardo",
      "Emilio",
      "Luis",
      "Gabriel"
    ).toDF("nombre")

    val resultado = ejercicio1(estudiantesData)

    checkDf(salidaesperada, resultado)
  }
}
