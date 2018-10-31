pipeline {
    agent any

    stages {
        stage ('Compile Stage') {
            steps {
            sh 'setenv DISPLAY :0.0'
                sh '/bin/sbtnocolor compile universal:packageBin universal:publish'
            }
        }
    }
}