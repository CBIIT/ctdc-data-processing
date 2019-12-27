const config = require('../config.json')

//Read the configuration parameters from the config file
var AWS = require('aws-sdk'),
		region = config.region,
		secretName = config.secretName,
		secret,
		decodedBinarySecret;

//Function that returns a promise with the secret specified by the AWS object
function getSecrets(){
	return new Promise((resolve, reject)=>{
		

		// Create a Secrets Manager client
		var client = new AWS.SecretsManager({
		    region: region
		});
			client.getSecretValue({SecretId: secretName}, function(err, data) {
		    if (err) {
		        if (err.code === 'DecryptionFailureException')
		            // Secrets Manager can't decrypt the protected secret text using the provided KMS key.
		            // Deal with the exception here, and/or rethrow at your discretion.
		            reject(err) ;
		        else if (err.code === 'InternalServiceErrorException')
		            // An error occurred on the server side.
		            // Deal with the exception here, and/or rethrow at your discretion.
		            reject(err) ;
		        else if (err.code === 'InvalidParameterException')
		            // You provided an invalid value for a parameter.
		            // Deal with the exception here, and/or rethrow at your discretion.
		            reject(err) ;
		        else if (err.code === 'InvalidRequestException')
		            // You provided a parameter value that is not valid for the current state of the resource.
		            // Deal with the exception here, and/or rethrow at your discretion.
		            reject(err) ;
		        else if (err.code === 'ResourceNotFoundException')
		            // We can't find the resource that you asked for.
		            // Deal with the exception here, and/or rethrow at your discretion.
		            reject(err) ;
		    }
		    else {
		        // Decrypts secret using the associated KMS CMK.
		        // Depending on whether the secret is a string or binary, one of these fields will be populated.
		        if ('SecretString' in data) {
		            secret = JSON.parse(data.SecretString);
		            resolve(secret);
		            } 
		        else {
		            let buff = new Buffer(data.SecretBinary, 'base64');
		            decodedBinarySecret = buff.toString('ascii');
		            resolve(decodedBinarySecret);
		        	}
		    }
		    //console.log(secret);

		     
			});

})

}

module.exports.getSecrets=getSecrets;
