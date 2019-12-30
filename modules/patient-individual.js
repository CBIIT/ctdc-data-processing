const request = require('request');
const config = require('../config.json');

const HTTP_OK=200;
const HTTP_CREATED=201;
const DOWNLOAD_URL_SUBPATH='/download_url';
const matchBaseUrlPatient=config.matchUatBaseUrlPatient;

/*This function returns a list of Map Objects with Key as Patient Id
 and a list of signed Urls. The input is the PatientId, list of S3 
 file Paths and a Bearer token
*/
async function getPatientSignedUrlList(patientId,s3PathList,token){
	//console.log(s3PathList);
	promiseList=[];
	s3PathList.forEach(element => {
		promise= getPatientSingleSignedURLs(patientId,element,token);
		promiseList.push(promise);
	});
	signedUrlList= await Promise.all(promiseList);
	map = new Map();
	map.set(patientId, signedUrlList);
	return map;
	//console.log(signedUrlList);
	
}

/*
This function returns a promise with the input as the PatientId, an S3 
 file Path and a Bearer token
*/
function getPatientSingleSignedURLs(patientId,s3path,token){
	return new Promise((resolve, reject)=>{
		url= matchBaseUrlPatient+patientId+DOWNLOAD_URL_SUBPATH;
		//console.log(url);
		//let s3Url=JSON.stringify({s3_url:s3path});
		let s3Url={"s3_url":s3path};
		
		request({
			url: url,
			headers: {
				'Authorization': token,
				'Content-Type': 'application/json' 
			 },
			 method: "POST",
			 json: true,
			 body: s3Url
		},(err,res)=>{
			if(err) {
		        console.error('Error is: '+err);
		        reject(err);
		    }else{
				if(res.statusCode != HTTP_CREATED){
					console.log('Response Code from Match for receiving Signed URL Info: '+res.statusCode)
					reject(res);
				}
				
				resolve(res.body.download_url);
		        //console.log(resBody);

			}
		});
	});

}

function getPatientPromise(patientId, token, projection, filter, projfilter){
	return new Promise((resolve, reject)=>{
		
		
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
/* Testtoken='Bearer eyJraWQiOiJrWWliVEpOaDFwQ0dJNnZLaVd2Z094a1VpUC1JN0ZfLXdkeXdxNkx1ay13IiwiYWxnIjoiUlMyNTYifQ.eyJ2ZXIiOjEsImp0aSI6IkFULkF5TmpWWkxRM0ViTW1xNGt1ZVRMb2tEcjBiMm5ILVRkRUR1OVc4cGMwQkEiLCJpc3MiOiJodHRwczovL2Jpb2FwcGRldi5va3RhLmNvbS9vYXV0aDIvYXVzMWh2MnZlNmZTZG0xdTcyOTciLCJhdWQiOiJhZHVsdG1hdGNoX3VhdCIsImlhdCI6MTU3NzcyMTY0OSwiZXhwIjoxNTc3NzY0ODQ5LCJjaWQiOiIwb2ExaHY0eTB6TEEwcWk1QjI5NyIsInVpZCI6IjAwdTJwN3A0cm5QN2IyUENQMjk3Iiwic2NwIjpbIm9wZW5pZCIsInByb2ZpbGUiXSwic3ViIjoiYW1pdC5tdWtoZXJqZWVAbmloLmdvdiIsIm5hbWUiOiJBbWl0IE11a2hlcmplZSJ9.fOaPj5_pbRThXc4gxF2Lnyvd5JsFDQJOujKQu30E1wls22_nYd1fc8PhEW_jt7LQPXjzs9lj25ll2dAIZZinN9Q21e6oBAnOk33R49eCNsAA2Ox2HFqYCeKMKlKg3gd5LJk4RIqLeV5Rtr6SFWlx7A62UVzEZr7qKK3KXP09v6lP5KWldOfMIeDW9TBl3FzeOo2b5J5I_zvTmG4ebK1P_WQtaEbkYPeL7Ywglmo3JfQxI4dEBpjaT2328aIXpGfEQtPpYIWdL9gJIxISekHr7YS8Zlf7o86D2RlUZLWpWqbvZAoyXahTUGKEgo-kusbokE0JTDWRMjeYFk9S6_xG-w'
TestpatientId='10002';
Tests3Path='s3://IR_WAO85/MSN-10002/testEmail10002/dna_sample.bam';
getPatientSignedURLs(TestpatientId,Tests3Path,Testtoken).then((result)=>{
	console.log('SignedURL: '+ (result));
}).catch((error)=>{
	console.log('SignedURLError: '+ JSON.stringify(error));
}); */
/* Testtoken='Bearer eyJraWQiOiJrWWliVEpOaDFwQ0dJNnZLaVd2Z094a1VpUC1JN0ZfLXdkeXdxNkx1ay13IiwiYWxnIjoiUlMyNTYifQ.eyJ2ZXIiOjEsImp0aSI6IkFULkF5TmpWWkxRM0ViTW1xNGt1ZVRMb2tEcjBiMm5ILVRkRUR1OVc4cGMwQkEiLCJpc3MiOiJodHRwczovL2Jpb2FwcGRldi5va3RhLmNvbS9vYXV0aDIvYXVzMWh2MnZlNmZTZG0xdTcyOTciLCJhdWQiOiJhZHVsdG1hdGNoX3VhdCIsImlhdCI6MTU3NzcyMTY0OSwiZXhwIjoxNTc3NzY0ODQ5LCJjaWQiOiIwb2ExaHY0eTB6TEEwcWk1QjI5NyIsInVpZCI6IjAwdTJwN3A0cm5QN2IyUENQMjk3Iiwic2NwIjpbIm9wZW5pZCIsInByb2ZpbGUiXSwic3ViIjoiYW1pdC5tdWtoZXJqZWVAbmloLmdvdiIsIm5hbWUiOiJBbWl0IE11a2hlcmplZSJ9.fOaPj5_pbRThXc4gxF2Lnyvd5JsFDQJOujKQu30E1wls22_nYd1fc8PhEW_jt7LQPXjzs9lj25ll2dAIZZinN9Q21e6oBAnOk33R49eCNsAA2Ox2HFqYCeKMKlKg3gd5LJk4RIqLeV5Rtr6SFWlx7A62UVzEZr7qKK3KXP09v6lP5KWldOfMIeDW9TBl3FzeOo2b5J5I_zvTmG4ebK1P_WQtaEbkYPeL7Ywglmo3JfQxI4dEBpjaT2328aIXpGfEQtPpYIWdL9gJIxISekHr7YS8Zlf7o86D2RlUZLWpWqbvZAoyXahTUGKEgo-kusbokE0JTDWRMjeYFk9S6_xG-w'
TestpatientId='10002';
Tests3Path='s3://IR_WAO85/MSN-10002/testEmail10002/dna_sample.bam';
getPatientSignedUrlList(TestpatientId,[Tests3Path,Tests3Path],Testtoken); */
module.exports ={
	getPatientPromise: getPatientPromise,
	getPatientSignedUrlList:getPatientSignedUrlList

};