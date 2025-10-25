pipeline {
  agent any
  options { timestamps() }
  environment { IMAGE = "pyspark-etl:%BUILD_NUMBER%" }

  stages {
    stage('Checkout') { steps { checkout scm } }

    stage('Build Docker Image') {
      steps { bat 'docker build -t %IMAGE% .' }
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
        bat '''
          docker run --rm ^
            -v "%WORKSPACE%\\input:/app/input" ^
            -v "%WORKSPACE%\\output:/app/output" ^
            %IMAGE% ^
            python /app/etl.py --input-dir /app/input --output-dir /app/output
        '''
      }
    }

    stage('Archive Artifacts') {
      steps { archiveArtifacts artifacts: 'output/**', fingerprint: true }
    }
  }
}
