const request = require('request');
const config = require('../config.json');
const matchBaseUrl=config.matchUatBaseUrl;
const HTTP_OK=200;
/*
This function gets a list of Patients for each arm specified by
armId. It also requires a Bearer OAuth Token which it uses for auth.
It returns a promise which returns a list of Map Objects with the key
being the armId and the value being an array of patient Ids
*/

function getPatientsByArmPromise(armId, token){
	return new Promise((resolve, reject) =>{
		request({
		  url: matchBaseUrl+armId,
		  headers: {
		     'Authorization': token
		  },
		  rejectUnauthorized: false
		}, function(err, res) {
		      if(err) {
		        console.error('Error is: '+err);
		        reject(err)
		      } else {
		      	if(res.statusCode != HTTP_OK){
		      		console.log('Response Code from Match for Patient List: '+res.statusCode)
		      		reject(res)
		      	}
		        //console.log(res.statusCode);
		        resBody= JSON.parse(res.body)
		        let patientList=getPatientList(resBody);
		        let map = new Map();
		        //Returns a Map object with Key as ARM Id and Values as Patient List
		        map.set(armId,patientList)
		        resolve(map);
		      }

		});

	})
}
/*
This function receives a JSON object from which the patient Ids are
extracted and stored in a new array which is returned
*/
function getPatientList(jsonObject){
  console.log(jsonObject.length)
  let arr=[];
  for (var i=0; i<jsonObject.length; i++){
    if(jsonObject[i].patientSequenceNumber){
      //console.log(jsonObject[i].patientSequenceNumber)
      arr.push(jsonObject[i].patientSequenceNumber);
    }
  }

  return arr;
}

module.exports.getPatientsByArmPromise=getPatientsByArmPromise;