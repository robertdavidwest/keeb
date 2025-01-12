# Keen

Connects to the Keen API, pulls down relavent metrics and sends the data to google sheets. `run.py` can be run on a schedule.


### Installation 

* You'll need to download the official [Keen IO Official Python Client Library](https://github.com/keenlabs/KeenClient-Python). You can do this with `pip install keen`
* Similarly you will need the [Google Spreadsheets Python API](https://github.com/burnash/gspread) - `pip install gspread`
* Now follow the instructions in the [gspread OAuth2 setup instructions](http://gspread.readthedocs.org/en/latest/oauth2.html) to install the necessary OAuth packages and obtain the needed google drive credentials in json format (Note that in the final step here, you will need to have an existing google sheets file to write to as you will need to share this file with your app to connect them)
* if you are going to add new documents later:
    * login to console.developers.com to see your google app set up that connects with gsdrive
    * click on Service Accounts
    * look for the e-mail associated with the app you have set up for this project
    * you will need to give adit access to this email in the new document so gsspread can write to it 

* Install `pandas`: `pip install pandas` (used to combine queries from keen)
* Install `keyring` (`pip install keyring`). We will use `keyring` to hold our auth credentials for both keen and google


### Setup

* Log in to your [keen.io](keen.io) account to obtain your project API Credentials. Navigate to your project copy your PROJECT ID, then click on **Show API Keys** to obtain your READ KEY
* You can either load the Keen client using a json file, or store your keen credentials in the keyring:
		
		import keyring
		
		project_id = 'ABC'
		read_key = 'XYZ' 
		service_name = 'keen_credentials'
		
		keyring.set_password(service_name, 'project_id', project_id),
	    keyring.set_password(service_name, 'read_key', read_key)
	 
* Similarly you can load the gdrive client using a json file or store your google drive credentials in the keyring:

		import keyring

	    credentials_json = 'keen-g-drive-credentials.json'
	    credentials = open(credentials_json, 'r').read()
		service_name = 'g-drive-keen-credentials'
			    
	    keyring.set_password(service_name, 'credentionals_json', credentials)

* Thats it. Now just adjust the metrics in `run.get_keen_data()` and the inputs in `if __name__ == '__main__'` to run
