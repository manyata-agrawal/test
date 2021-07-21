pipeline {
  agent any
  stages {
    stage('SBT Clean') {
      steps {
        sh 'sbt clean'
      }
    }

    stage('SBT Compile') {
      steps {
        sh 'sbt compile'
      }
    }

    stage('SBT Publish to Local') {
      steps {
        sh 'sbt docker:publishLocal'
      }
    }

    stage('Docker Login') {
      steps {
        sh 'docker login -u soorkod -p hudY4tI8x1TSufKkXyKO%'
      }
    }

    stage('Docker Tag Image') {
      steps {
        sh 'docker tag bodhee_jdbc_gateway_connector:1.0.3 soorkod/bodhee:bodhee_jdbc_connector-${APP_VERSION}'
      }
    }

    stage('Docker Push') {
      steps {
        sh 'docker push soorkod/bodhee:bodhee_jdbc_connector-${APP_VERSION}'
      }
    }

    stage('Docker Cleanup') {
      steps {
        sh 'docker rmi bodhee_jdbc_gateway_connector:1.0.3'
      }
    }

  }
  environment {
    APP_VERSION = '1.0.3'
  }
}