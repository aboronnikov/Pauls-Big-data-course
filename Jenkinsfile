pipeline {
    agent any

    stages {
        stage ('Compile Stage') {
            steps {
                sh '/bin/sbtnocolor compile'
                sh '/bin/sbtnocolor package'
                sh '/bin/sbtnocolor publish'
            }
        }
    }
}