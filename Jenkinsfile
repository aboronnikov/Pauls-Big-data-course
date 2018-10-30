pipeline {
    agent any

    stages {
        stage ('Compile Stage') {
            ansiColor {
                sbt compile, package, publish
            }
        }
    }
}