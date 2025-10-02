// data_aggregator.js

import express from "express";
import cors from "cors";
import mongoose from "mongoose";
import { config } from "dotenv";

config(); // Load environment variables from .env

const app = express();
const PORT = process.env.PORT || 5002;

// --- DATABASE CONNECTION CONFIGURATION ---

// 1. Connection for READING (Source DB: 'test', as used in your model files)
const sourceDB = mongoose.createConnection(process.env.MONGO_URI, { dbName: 'test' });
sourceDB.on('error', (err) => console.error("Source DB Connection Error:", err));
sourceDB.once('open', () => console.log("✅ Source MongoDB Connected (DB: test)"));

// 2. Connection for WRITING (Target DB: 'janvaani_aggregated_data', as requested)
const TARGET_DB_NAME = 'janvaani_aggregated_data'; 
const targetDB = mongoose.createConnection(process.env.MONGO_URI, { dbName: TARGET_DB_NAME });
targetDB.on('error', (err) => console.error("Target DB Connection Error:", err));
targetDB.once('open', () => console.log(`✅ Target MongoDB Connected (DB: ${TARGET_DB_NAME})`));


// --- SCHEMAS & MODELS (Bound to Source DB - based on server.js) ---

// 1. Detection Schema (renamed to link via issueId)
const DetectionSchema = new mongoose.Schema({
    issueId: { type: mongoose.Schema.Types.ObjectId, ref: 'Issue', required: true },
    annotatedImageUrl: String,
    detections: Array,
    createdAt: Date,
}, { collection: 'detections' }); // Must specify collection name for the Model bound to sourceDB

// 2. Issue Schema (from server.js)
const IssueSchema = new mongoose.Schema({
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    title: String,
    description: String,
    location: String,
    coordinates: {
        latitude: Number,
        longitude: Number
    },
    status: { type: String, default: 'pending' },
    imageUrl: String,
    submittedBy: String,
    redFlags: { type: Number, default: 0 },
    greenFlags: { type: Number, default: 0 },
}, { timestamps: true });

// 3. Flag Schema (from server.js)
const FlagSchema = new mongoose.Schema({
    issueId: { type: mongoose.Schema.Types.ObjectId, ref: 'Issue', required: true },
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    userName: { type: String, required: true },
    userEmail: { type: String, required: true },
    type: { type: String, enum: ['red', 'green'], required: true },
    reason: { type: String },
}, { timestamps: true });

// 4. User Schema (from server.js)
const UserSchema = new mongoose.Schema({
    name: { type: String, required: true },
    email: { type: String, required: true, unique: true },
    phone: { type: String, required: true },
    otp: String,
    otpExpiry: Date,
    verified: { type: Boolean, default: false },
    faceImageUrl: { type: String, default: "" },
    points: { type: Number, default: 10000 },
});

// Models bound to the sourceDB connection
const User = sourceDB.model("User", UserSchema);
const Issue = sourceDB.model("Issue", IssueSchema);
const Flag = sourceDB.model("Flag", FlagSchema); // Mongoose defaults to 'flags' or use 'flogs' if necessary in $lookup
const Detection = sourceDB.model("Detection", DetectionSchema); // Mongoose defaults to 'detections'


// --- NEW MODEL FOR SAVING AGGREGATED DATA ---

// This schema defines the structure of the final denormalized object with all fields.
const AggregatedIssueSchema = new mongoose.Schema({
    // --- Issue Fields ---
    _id: mongoose.Schema.Types.ObjectId, 
    sourceIssueId: mongoose.Schema.Types.ObjectId, // For clarity
    title: String,
    description: String,
    location: String,
    coordinates: { latitude: Number, longitude: Number },
    status: String,
    imageUrl: String,
    submittedBy: String,
    redFlags: Number,
    greenFlags: Number,
    createdAt: Date,
    updatedAt: Date,

    // --- Embedded Detection Data (First Match) ---
    annotatedImageUrl: String,
    detections: Array,
    detectedAt: Date,
    
    // --- Embedded Submitting User Details ---
    submittedUser: {
        sourceUserId: mongoose.Schema.Types.ObjectId,
        name: String,
        email: String,
        phone: String,
        points: Number,
        faceImageUrl: String,
    },
    
    // --- Embedded Flag Data (Full Array) ---
    issueFlags: [{
        sourceFlagId: mongoose.Schema.Types.ObjectId,
        userId: mongoose.Schema.Types.ObjectId,
        userName: String,
        userEmail: String,
        type: String,
        reason: String,
        createdAt: Date,
    }],
    
}, { collection: 'agrigate' }); // Target collection name

// Model bound to the targetDB connection
const AggregatedIssue = targetDB.model("AggregatedIssue", AggregatedIssueSchema);


// --- REUSABLE AGGREGATION AND SAVE FUNCTION ---
async function fetchAggregateAndSaveData() {
    console.log(`\n--- STARTING AGGREGATION AND SAVE at ${new Date().toLocaleTimeString()} ---`);
    
    try {
        // 1. Aggregation Pipeline
        const issuesToSave = await Issue.aggregate([
            // 1. Lookup the submitting User details
            {
                $lookup: { from: 'users', localField: 'userId', foreignField: '_id', as: 'submittedUserArr' }
            },
            { $unwind: { path: '$submittedUserArr', preserveNullAndEmptyArrays: true } },

            // 2. Lookup all Flags for this Issue
            {
                // NOTE: Using 'flogs' if that is the actual collection name for Flags
                $lookup: { from: 'flogs', localField: '_id', foreignField: 'issueId', as: 'issueFlags' } 
            },

            // 3. Lookup Detection Data (1-to-1 or 1-to-many, we'll take the first)
            {
                $lookup: { from: 'detections', localField: '_id', foreignField: 'issueId', as: 'detectionData' }
            },

            // 4. Project the final, flattened structure
            {
                $project: {
                    // ISSUE FIELDS
                    sourceIssueId: '$_id',
                    title: 1, description: 1, location: 1, coordinates: 1,
                    status: 1, imageUrl: 1, submittedBy: 1, redFlags: 1, greenFlags: 1,
                    createdAt: 1, updatedAt: 1,

                    // EMBEDDED DETECTION FIELDS (from the first detection document)
                    annotatedImageUrl: { $arrayElemAt: ['$detectionData.annotatedImageUrl', 0] },
                    detections: { $arrayElemAt: ['$detectionData.detections', 0] },
                    detectedAt: { $arrayElemAt: ['$detectionData.createdAt', 0] },

                    // EMBEDDED USER FIELDS (flattened)
                    submittedUser: {
                        sourceUserId: '$submittedUserArr._id',
                        name: '$submittedUserArr.name',
                        email: '$submittedUserArr.email',
                        phone: '$submittedUserArr.phone',
                        points: '$submittedUserArr.points',
                        faceImageUrl: '$submittedUserArr.faceImageUrl',
                    },

                    // EMBEDDED FLAGS ARRAY
                    issueFlags: {
                        $map: {
                            input: '$issueFlags',
                            as: 'flag',
                            in: {
                                sourceFlagId: '$$flag._id',
                                userId: '$$flag.userId',
                                userName: '$$flag.userName',
                                userEmail: '$$flag.userEmail',
                                type: '$$flag.type',
                                reason: '$$flag.reason',
                                createdAt: '$$flag.createdAt',
                            }
                        }
                    },
                }
            },
            { $sort: { createdAt: -1 } }
        ]);

        // 2. Get Metadata
        const users = await User.find({}).lean();
        const flags = await Flag.find({}).lean();
        
        const aggregatedData = {
            metadata: {
                timestamp: new Date().toISOString(),
                totalIssues: issuesToSave.length,
                totalUsers: users.length,
                totalFlags: flags.length,
            },
            issues: issuesToSave,
        };


        // 3. Save the aggregated data to the new collection ('agrigate')
        
        // Clear the existing data
        await AggregatedIssue.deleteMany({});
        
        // Insert the new aggregated documents
        if (issuesToSave.length > 0) {
            await AggregatedIssue.insertMany(issuesToSave);
        }

        console.log(`--- SAVE COMPLETE --- Inserted ${issuesToSave.length} documents into DB: ${TARGET_DB_NAME}, Collection: agrigate`);
        
        return aggregatedData;
        
    } catch (error) {
        console.error("Error during aggregation process:", error);
        throw new Error("Failed to fetch, aggregate, or save data to the target DB.");
    }
}


// --- MIDDLEWARE & ROUTES (Standard Express setup) ---

app.use(cors());
app.use(express.json());


// --- AGGREGATION ENDPOINT (Called by the Sync button) ---
app.get('/api/sync', async (req, res) => {
    try {
        const aggregatedData = await fetchAggregateAndSaveData();
        
        console.log("First saved document (partial view):", aggregatedData.issues[0]);

        res.json({ 
            success: true, 
            message: `Successfully saved ${aggregatedData.metadata.totalIssues} aggregated issues to DB: ${TARGET_DB_NAME}, Collection: agrigate.`,
            metadata: aggregatedData.metadata
        });

    } catch (error) {
        console.error("Error in /api/sync:", error);
        res.status(500).json({ success: false, error: error.message || "Server error during data aggregation." });
    }
});


// --- ROOT ROUTE (Serves the HTML Page) ---
app.get('/', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Data Aggregator Sync Panel</title>
            <style>
                body { font-family: sans-serif; display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100vh; margin: 0; background-color: #f4f4f9; }
                .container { background: white; padding: 40px; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); text-align: center; }
                h1 { color: #333; }
                #syncButton { 
                    padding: 10px 20px; 
                    font-size: 16px; 
                    background-color: #007bff; 
                    color: white; 
                    border: none; 
                    border-radius: 5px; 
                    cursor: pointer; 
                    transition: background-color 0.3s; 
                }
                #syncButton:hover:not(:disabled) { background-color: #0056b3; }
                #syncButton:disabled { background-color: #a0c3ff; cursor: not-allowed; }
                #statusMessage { margin-top: 20px; font-size: 1.1em; }
                .success { color: #28a745; }
                .error { color: #dc3545; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Janvaani Data Aggregator</h1>
                <p>Click the button to fetch, combine, and save all data into the new **agrigate** collection.</p>
                <button id="syncButton">Sync & Save All Data</button>
                <div id="statusMessage"></div>
            </div>

            <script>
                document.getElementById('syncButton').addEventListener('click', async () => {
                    const button = document.getElementById('syncButton');
                    const status = document.getElementById('statusMessage');

                    button.disabled = true;
                    status.innerHTML = 'Syncing... Please check the server console for progress.';
                    status.className = '';

                    try {
                        const response = await fetch('/api/sync');
                        const data = await response.json();

                        if (data.success) {
                            status.innerHTML = `\${data.message}<br>Total Issues: \${data.metadata.totalIssues} saved.`;
                            status.className = 'success';
                        } else {
                            status.innerHTML = data.error || 'Sync failed due to an unknown error.';
                            status.className = 'error';
                        }
                    } catch (error) {
                        status.innerHTML = 'Network error: Could not connect to the aggregation server.';
                        status.className = 'error';
                        console.error('Frontend sync error:', error);
                    } finally {
                        button.disabled = false;
                    }
                });
            </script>
        </body>
        </html>
    `);
});


// --- START SERVER ---
app.listen(PORT, () => console.log(`🚀 Data Aggregator running at http://localhost:${PORT}`));
