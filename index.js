const express = require('express');
const app = express();
const http = require('http').Server(app);
const path = require('path');
const stream = require('stream');
const fs = require('fs');
const csv = require('csv-parser');
const mongoose = require('mongoose');

// Connect to MongoDB
mongoose.connect('mongodb://localhost:27017/uniqueContacts', {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    maxPoolSize: 50, // Set the maximum number of connections in the pool
})
    .then(() => {
        console.log('Connected to MongoDB');
        
    })
    .catch((error) => {
        console.error('Error connecting to MongoDB:', error);
    })
    
           

// Define a Schema for your data
const DataSchema = new mongoose.Schema({
    email: { type: String },
    phone: {type: String, unique: true, required: true},

}, {
    timestamps: true,
}); // strict: false allows flexible document structure

const DataModel = mongoose.model('contacts', DataSchema);

console.log('Reading CSV file...');

app.get('/', async (req, res) => {
    try {
        const dataFolderPath = path.join(__dirname, 'Data');
        const files = await readCSV(dataFolderPath);
        console.log(">>> Files to process:", files);  

        for (let i=0; i<files.length; i++) {
            const file = files[i];
            console.log(`Processing file: ${file}`);
            
            // Process each file and wait for it to complete
            await new Promise((resolve, reject) => {
                const results = [];
                fs.createReadStream(file)
                    .pipe(csv(
                        {
                            delimited: [',', ';'],
                            trim: true,
                            columns: true,
                        }
                    ))
                    .on('data', (row) => {
                        // Check if we got a single column (indicating wrong separator)
                        if(Object.keys(row).length <= 1){
                            let keys = Object.keys(row);
                            const strObjectKeys = keys[0];
                            const allValues = row[strObjectKeys].split(';').map(value => value.trim().replaceAll('"', '').replaceAll('\'', ''));
                            const allKeys = strObjectKeys.split(';').map(value => value.trim().replaceAll('"', '').replaceAll('\'', ''));
                            const newRow = {};
                            for(let i=0; i<allKeys.length; i++){
                                newRow[allKeys[i]] = allValues[i];
                            }
                            results.push(newRow);
                        }else{
                            results.push(row);
                        }
                    })
                    .on('end', async () => {
                        console.log(`File ${file} data:`, results);
                        for(let i=0; i<results.length; i++){
                            try{
                                const possiblePhoneFields = ['phone', 'phone_number', 'phoneNumber', 'mobile', 'mobile_number', 'mobileNumber', 'contact', 'contact_number'];
                                const phoneField = possiblePhoneFields.find(field => results[i][field]);

                                const possibleEmailFields = ['email', 'email_address', 'emailAddress', 'mail'];
                                const emailField = possibleEmailFields.find(field => results[i][field]);

                                const dataRow = {
                                    email : results[i][emailField],
                                    phone : results[i][phoneField]
                                };
                                if(!dataRow.phone){
                                    console.log(dataRow , results[i]);
                                }
                            
                                const newData = new DataModel(dataRow);
                                await newData.save();
                            }catch(err){
                                console.error(`Error saving row: ${err}`);
                            }
                        }
                        resolve();
                    })
                    .on('error', (error) => {
                        console.error(`Error processing file ${file}:`, error);
                        reject(error);
                    });
            });
        }
        
        res.send('All files processed successfully');
    } catch (error) {
        console.error('Error processing files:', error);
        res.status(500).send('Error processing files');
    }
});

const readCSV = async (filePath) => {
    //if it is a file return the file object
    //if it is a folder return the files in the folder
    const files = fs.readdirSync(filePath);
    const fileList = [];
    for(const file of files){
        let filePathTemp = path.join(filePath, file);
        if(fs.lstatSync(filePathTemp).isDirectory()){
            const nestedFiles = await readCSV(filePathTemp);
            fileList.push(...nestedFiles);
        }else{
            // If file has csv extension
            if(file.endsWith('.csv')){
                fileList.push(filePathTemp);
            }
        }
    }
    return fileList;
}


const processBatch = async (batch) => {
    try {
        console.log(`Processing batch of ${batch.length} records...`);
        if(!global.really){
            global.really = true;
            console.log("Batch processing started" ,  batch);
        }
        // Save batch of contacts to MongoDB
        const savedContacts = await DataModel.insertMany(batch, { ordered: false });
        console.log(`Successfully saved ${savedContacts.length} records to MongoDB`);       
        return savedContacts;
    } catch (error) {
        console.error('Error processing batch:', error);
        if (error.writeErrors) {
            console.log(`Number of write errors: ${error.writeErrors.length}`);
            error.writeErrors.forEach(writeError => {
                console.log('Write error:', writeError.errmsg);
            });
        }
        throw error;
    }
};

const port = 8000;
http.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});
