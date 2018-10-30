pipeline {
    agent any

    stages {
        stage ('Compile Stage') {
            ansiColor('xterm') {
                sh 'sbt compile, package, publish'
            }
        }
    }
}