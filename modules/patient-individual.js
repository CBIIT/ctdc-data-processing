const request = require('request');
const config = require('../config.json');

const HTTP_OK=200;

function getPatientPromise(patientId, token, projection, filter, projfilter){
	return new Promise((resolve, reject)=>{
		
		const matchBaseUrlPatient=config.matchUatBaseUrlPatient;
		//console.log('PatientId: '+ patientId+ ' token '+ token)
		url= matchBaseUrlPatient+patientId+'?';
		//Creating the URL to send the Patient API request
		if(projection){
			url=url+'projection='+projection+'&';
		}
		if(filter){
			url=url+'filter='+filter+'&';
		}
		if(projfilter){
			url=url+'projfilter='+projfilter+'&';

		}
		
		request({
		  url: url,
		  headers: {
		     'Authorization': token
		  },
		  json: true
		},(err,res)=>{

			if(err) {
		        console.error('Error is: '+err);
		        reject(err)
		      }else{
		      	if(res.statusCode != HTTP_OK){
		      		console.log('Response Code from Match for Patient Info: '+res.statusCode)
		      		reject(res)
		      	}else{
		      		//console.log(res.statusCode);
		        resBody= (res.body);
		        //console.log(resBody);()
		        fileList=getS3PathsForPatient(resBody);
		        let map = new Map();
		        //Returns a Map object with Key as ARM Id and Values as Patient List
		        map.set(patientId,fileList)
		        resolve(map);

		      	}
		      }

		});
	});

}


/*
This function expects data in the format as below
{
    "biopsies": [
        {
            "nextGenerationSequences": [
                {
                    "status": "REJECTED",
                    "ionReporterResults": {
                        "dnaBamFilePath": "s3://IR_WAO85/MSN-10002/jobTestMatt/18148standard.bam",
                        "rnaBamFilePath": "s3://IR_WAO85/MSN-10002/jobTestMatt/18148standardrna.bam",
                        "vcfFilePath": "s3://IR_WAO85/MSN-10002/jobTestMatt/18148-standard.vcf"
                    }
                },
                {
                    "status": "CONFIRMED",
                    "ionReporterResults": {
                        "dnaBamFilePath": "s3://IR_WAO85/MSN-10002/annatest1/IonXpress_045_rawlib.bam",
                        "rnaBamFilePath": "s3://IR_WAO85/MSN-10002/annatest1/IonXpress_046_rawlib_merged.bam",
                        "vcfFilePath": "s3://IR_WAO85/MSN-10002/annatest1/MSN65383_v1_MSN65383_RNA_v1_Non-Filtered_2019-05-10_21:00:15.vcf"
                    }
                },
            ] 
It filters for elements fields where the "status" is "CONFIRMED" and returns the "dnaBamFilePath", "rnaBamFilePath" ,vcfFilePath in 
an array s3PathList           
*/
function getS3PathsForPatient(data){
	//console.log(data.biopsies[0].nextGenerationSequences[0]);
	biopsiesLen= data.biopsies.length;
	s3PathList=[];
	for(i=0; i<biopsiesLen;i++){
		ngsLength=data.biopsies[i].nextGenerationSequences.length;
		for(j=0;j<ngsLength;j++){
			status=data.biopsies[i].nextGenerationSequences[j].status;
			//console.log(status)
			if(status=='CONFIRMED'){
				if(data.biopsies[i].nextGenerationSequences[j].ionReporterResults.dnaBamFilePath){
					//console.log(data.biopsies[i].nextGenerationSequences[j].ionReporterResults.dnaBamFilePath);
					s3PathList.push(data.biopsies[i].nextGenerationSequences[j].ionReporterResults.dnaBamFilePath);
				}
				if(data.biopsies[i].nextGenerationSequences[j].ionReporterResults.rnaBamFilePath){
					s3PathList.push(data.biopsies[i].nextGenerationSequences[j].ionReporterResults.rnaBamFilePath);
				}
				if(data.biopsies[i].nextGenerationSequences[j].ionReporterResults.vcfFilePath){
					s3PathList.push(data.biopsies[i].nextGenerationSequences[j].ionReporterResults.vcfFilePath);

				}
			}
		}

	}

	return s3PathList;
}
module.exports.getPatientPromise=getPatientPromise;