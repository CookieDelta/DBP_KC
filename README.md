Final project on Big Data Processing on KeepCoding

Tree for better understanding of the project structure, files to review are examen.scala and all the others inside examen/ folder under test directory

src/
├─ main/                 # Main application code
│  ├─ scala/             # Scala source code
│  │  ├─ job/           # Job package
│  │  │  ├─ examen/     # Examen package
│  │  │  │  ├─ examen.scala   # Main examen Scala file
│  
├─ test/                 # Test files
│  ├─ scala/             # Scala test code
│  │  ├─ job/           # Job package for tests
│  │  │  ├─ examen/     # Examen package for tests
│  │  │  │  ├─ TestE1.scala   # Test for exercise 1
│  │  │  │  ├─ TestE2.scala   # Test for exercise 2
│  │  │  │  ├─ TestE3.scala   # Test for exercise 3
│  │  │  │  ├─ TestE4.scala   # Test for exercise 4
│  │  │  │  ├─ TestE5.scala   # Test for exercise 5
│  │  ├─ resources/      # Test resources
│  │  │  ├─ ventas.csv   # CSV file used in tests
│  │  ├─ utils/          # Utility classes
│  │  │  ├─ TestInit.scala   # Test initialization code
