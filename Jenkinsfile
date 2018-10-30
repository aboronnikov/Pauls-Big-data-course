pipeline {
    agent any

    stages {
        stage ('Compile Stage') {
            ansiColor('xterm') {
                sbt compile, package, publish
            }
        }
    }
}