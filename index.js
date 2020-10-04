const mongo = require('mongodb'); 
const fastcsv = require('fast-csv');
const json2csv = require('json2csv');
const util = require('util');
const fs = require('fs');

const writeFile = util.promisify(fs.writeFile);

const args = process.argv.splice(2, process.argv.length - 1);
const getArgument = (label) => {
    let e =  args.find(a => {
        return a.includes(`--${label}`);
    });

    if (e) {
        e = e.replace('--', '').split('=');

        return ((e.length > 1)) ? {val: e[1]} : e[0];
    }

    return undefined;
};

const dotNotate = (obj,target,prefix) => {
    target = target || {},
    prefix = prefix || "";
  
    Object.keys(obj).forEach((key) => {
      if ( typeof(obj[key]) === "object" ) {
        dotNotate(obj[key],target,prefix + key + ".");
      } else {
        return target[prefix + key] = obj[key];
      }
    });
  
    return target;
};

const start = async () => {
    let client;
    try {
        client = await mongo.connect('mongodb://localhost:27017/', {
            useNewUrlParser: true, useUnifiedTopology: true
        });
    } catch(err) {
        return console.log('DB connection failed', err);
    }

    const allDb = (await client.db('all').admin().listDatabases()).databases.map(e => {
        return e.name;
    });

    // is db exists
    if (!allDb.includes(getArgument('db').val)) {
        console.log(`DB ${getArgument('db').val} does not exists`);
        return client.close();
    }

    const allCollection = (await client.db(getArgument('db').val).listCollections().toArray()).map(e => {
        return e.name;
    });

    const targetCollection = getArgument('collection');

    // is collection(s) exists
    const isCollectionsExists = targetCollection.val.split(',').every((val) => allCollection.includes(val))
    if (!isCollectionsExists) {
        console.log('given collection does not exists');
        return client.close();
    }    

    const db = client.db(getArgument('db').val);
    let timeRange = getArgument('day') || getArgument('week') || getArgument('month') || 0;

    if (timeRange) {
        switch (timeRange) {
            case 'day':
                timeRange = 1000 * 60 * 60 * 24;
                break;
            case 'week':
                timeRange = 1000 * 60 * 60 * 24 * 7;
                break;
            case 'month':
                timeRange = 1000 * 60 * 60 * 24 * 31;
                break;
            default:
                timeRange = 0;
        }
    }

    let totalDataFound = 0;
    const exportCollection = async (name) => {
        const data = await db.collection(name).find({
            timestamp: {
                $gte: Date.now() - timeRange
            }
        }, {
            projection: { _id: 0, __v: 0 }
        }).toArray();

        let res = [];
        data.forEach(d => {
            res.push(dotNotate(d));
        });

        totalDataFound += data.length;

        if (data.length > 0) {
            const fields = Object.keys(dotNotate(data[0]));

            const csv = await json2csv.parseAsync(res, fields);
    
            return writeFile(`${name}.csv`, csv);
        }
    };

    console.log('Collection data and exporting data to csv...');

    if (targetCollection) {
        try {
            if (targetCollection.val.includes(',')) {
                for await (const e of targetCollection.val.split(',')) {
                    await exportCollection(e);
                }
            } else {
                await exportCollection(targetCollection.val);
            }
            
        } catch(err) {
            console.log('Error exporting data to csv', err);
        }
    } else {
        for await (const e of allCollection) {
            await exportCollection(e);
        }
    }

    console.log(`Done, ${totalDataFound} data found!`);

    return client.close();
};

start();