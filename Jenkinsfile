pipeline { 
    environment { 
        registry = "vixion/airflowrepo" 
        registryCredential = 'dockerhub_id' 
        dockerImage = '' 
    }
    agent any 
    stages { 
        stage('Cloning our Git') { 
            steps { 
                // git 'https://github.com/TMCreme/dbt_airflow_project.git' 
                checkout([$class: 'GitSCM', 
                    branches: [[name: '*/deploy']], 
                    doGenerateSubmoduleConfigurations: false, 
                    extensions: [[$class: 'RelativeTargetDirectory', 
                        relativeTargetDir: 'checkout-directory']], 
                    submoduleCfg: [], 
                    userRemoteConfigs: [[url: 'https://github.com/TMCreme/dbt_airflow_project.git']]])
            }
        } 
        stage('Building our image') { 
            steps { 
                script { 
                    dockerImage = docker.build registry + ":$BUILD_NUMBER" 
                }
            } 
        }
        stage('Deploy our image') { 
            steps { 
                script { 
                    docker.withRegistry( '', registryCredential ) { 
                        dockerImage.push() 
                    }
                } 
            }
        } 
        stage('Cleaning up') { 
            steps { 
                sh "docker rmi $registry:$BUILD_NUMBER" 
            }
        } 
    }
}