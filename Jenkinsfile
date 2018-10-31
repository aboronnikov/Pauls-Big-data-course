pipeline {
    agent any

    stages {
        stage ('Compile Stage') {
            steps {
                sh '/bin/sbtnocolor compile universal:packageBin universal:publish'
            }
        }
    }
}