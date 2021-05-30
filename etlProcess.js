const MongoClient = require('mongodb').MongoClient;
const config = require('./config.js');
const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');

const uri = `mongodb+srv://${config.user}:${config.password}@ctcluster.fiphl.mongodb.net/myFirstDatabase?retryWrites=true&w=majority`;


const client = new MongoClient(uri, {
    useNewUrlParser: true, useUnifiedTopology: true
});

async function run() {
    try {
        await client.connect();

        const db = client.db('sba');
        const collection = db.collection('ct');

        let files = fs.readdirSync('./data');
        for (let i = 0; i < files.length; i++) {
            let file = files[i];
            let loans = await(readCsv(file));
            const result = await collection.insertMany(loans);
            console.dir(result.insertedCount);
        }

    } finally {
        await client.close();
    }
};
async function readCsv(fileName) {
    let loans = [];
    return new Promise (resolve => {
        const readStream = fs.createReadStream(path.join(__dirname, 'data', fileName));
        readStream.pipe(csv.parse({headers: true, strictColumnHandling : true}))
          .on('error', error => {console.log("error: ", error)})
          .on('data', loan => {
                if (loan.BorrowerState === 'CT') {
                    loans.push(loan)
                }
              })
          .on('data-invalid', () => console.log('bad row, moving on'))
          .on('end', () => resolve(loans));
  });
}

run().catch(console.dir);