pipeline {
  agent any
  options { timestamps() }

  stages {
    stage('Checkout') { steps { checkout scm } }

    stage('Build Docker Image') {
      steps {
        // Groovy interpola ${env.BUILD_NUMBER} antes de llamar a cmd
        bat "docker build -t pyspark-etl:${env.BUILD_NUMBER} ."
      }
    }

    stage('Prepare I/O') {
      steps {
        bat '''
          if not exist input mkdir input
          if not exist output mkdir output
        '''
      }
    }

    stage('Run ETL') {
      steps {
        bat """
          docker run --rm ^
            -v "%WORKSPACE%\\input:/app/input" ^
            -v "%WORKSPACE%\\output:/app/output" ^
            pyspark-etl:${env.BUILD_NUMBER} ^
            python /app/etl.py --input-dir /app/input --output-dir /app/output
        """
      }
    }

    stage('Archive Artifacts') {
      steps { archiveArtifacts artifacts: 'output/**', fingerprint: true }
    }
  }
}
