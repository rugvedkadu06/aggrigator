// data_aggregator.js

import express from "express";
import cors from "cors";
import mongoose from "mongoose";
import { config } from "dotenv";

config(); // Load environment variables from .env

const app = express();
const PORT = process.env.PORT || 5002;

// --- DATABASE CONNECTION CONFIGURATION ---

// 1. Connection for READING (Source DB: 'test')
const sourceDB = mongoose.createConnection(process.env.MONGO_URI, { dbName: 'test' });
sourceDB.on('error', (err) => console.error("Source DB Connection Error:", err));
sourceDB.once('open', () => console.log("✅ Source MongoDB Connected (DB: test)"));

// 2. Connection for WRITING (Target DB: 'aggregated_db' or change to 'test' if preferred)
// Using a separate connection allows saving to a different DB, or you can point it to 'test'.
const TARGET_DB_NAME = 'janvaani_aggregated_data'; // Name of the new database
const targetDB = mongoose.createConnection(process.env.MONGO_URI, { dbName: TARGET_DB_NAME });
targetDB.on('error', (err) => console.error("Target DB Connection Error:", err));
targetDB.once('open', () => console.log(`✅ Target MongoDB Connected (DB: ${TARGET_DB_NAME})`));


// --- SCHEMAS & MODELS (Bound to Source DB) ---

// Schemas are defined normally, but models are created using the source connection.

const DetectionSubSchema = new mongoose.Schema({
    annotatedImageUrl: String,
    detections: Array,
    detectedAt: Date,
}, { _id: false });

const IssueSchema = new mongoose.Schema({
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
    title: String,
    description: String,
    location: String,
    imageUrl: String,
    submittedBy: String,
    redFlags: Number,
    greenFlags: Number,
    status: String,
    detection: DetectionSubSchema,
    priority: String, 
    assignedTo: String,
}, { timestamps: true });

const FlagSchema = new mongoose.Schema({
    issueId: { type: mongoose.Schema.Types.ObjectId, ref: 'Issue' },
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
    userName: String,
    type: String,
    reason: String,
}, { timestamps: true });

const UserSchema = new mongoose.Schema({
    name: String,
    email: String,
    phone: String,
    points: Number,
    faceImageUrl: String,
});

// Models bound to the sourceDB connection
const User = sourceDB.model("User", UserSchema);
const Issue = sourceDB.model("Issue", IssueSchema);
const Flag = sourceDB.model("Flag", FlagSchema); // Used for fetching all flags (Step 2)


// --- NEW MODEL FOR SAVING AGGREGATED DATA ---

// This schema defines the structure of the final denormalized object.
const AggregatedIssueSchema = new mongoose.Schema({
    _id: mongoose.Schema.Types.ObjectId, // Keep the original Issue _id
    title: String,
    description: String,
    status: String,
    imageUrl: String,
    submittedBy: String,
    
    // Embedded User Details
    submittedByEmail: String,
    
    // Embedded Detection Data
    annotatedImageUrl: String,
    detections: Array,
    detectedAt: Date,

    // Embedded Flag Data
    flags: Array,
    
    // Metadata
    createdAt: Date,
    updatedAt: Date,
}, { collection: 'agrigate' }); // Use the requested collection name: 'agrigate'

// Model bound to the targetDB connection
const AggregatedIssue = targetDB.model("AggregatedIssue", AggregatedIssueSchema);


// --- REUSABLE AGGREGATION AND SAVE FUNCTION ---
async function fetchAggregateAndSaveData() {
    console.log(`\n--- STARTING AGGREGATION AND SAVE at ${new Date().toLocaleTimeString()} ---`);
    
    try {
        // 1. Run the aggregation pipeline on the Issue model (sourceDB)
        const issuesToAggregate = await Issue.aggregate([
            {
                $lookup: { from: 'users', localField: 'userId', foreignField: '_id', as: 'submittedByUser' }
            },
            { $unwind: { path: '$submittedByUser', preserveNullAndEmptyArrays: true } },
            {
                // IMPORTANT: Using 'flogs' based on your DB screenshots.
                $lookup: { from: 'flogs', localField: '_id', foreignField: 'issueId', as: 'issueFlags' }
            },
            {
                $project: {
                    // Select and flatten all necessary fields for the new collection
                    _id: 1, title: 1, description: 1, location: 1, status: 1, 
                    redFlags: 1, greenFlags: 1, createdAt: 1, updatedAt: 1,
                    priority: 1, assignedTo: 1,
                    
                    // Image Logic
                    imageUrl: { $ifNull: ['$detection.annotatedImageUrl', '$imageUrl'] },
                    
                    // Embedded Detection Data
                    annotatedImageUrl: '$detection.annotatedImageUrl',
                    detections: '$detection.detections',
                    detectedAt: '$detection.detectedAt',

                    // Embedded User Details
                    submittedBy: '$submittedByUser.name',
                    submittedByEmail: '$submittedByUser.email',
                    
                    // Embedded Flags
                    flags: '$issueFlags',
                }
            },
            { $sort: { createdAt: -1 } }
        ]);

        // 2. Prepare the full aggregated response data (optional, for logging/metadata)
        const users = await User.find({}).lean();
        const flags = await Flag.find({}).lean();

        const aggregatedData = {
            metadata: {
                timestamp: new Date().toISOString(),
                totalIssues: issuesToAggregate.length,
                totalUsers: users.length,
                totalFlags: flags.length,
            },
            // Keeping the main result array separate for saving
            issues: issuesToAggregate,
        };


        // 3. Save the aggregated data to the new collection ('agrigate')
        
        // Clear the existing data in the target collection first
        await AggregatedIssue.deleteMany({});
        
        // Insert the new aggregated documents
        if (issuesToAggregate.length > 0) {
            await AggregatedIssue.insertMany(issuesToAggregate);
        }

        console.log(`--- SAVE COMPLETE --- Inserted ${issuesToAggregate.length} documents into '${TARGET_DB_NAME}.agrigate'`);
        
        return aggregatedData;
        
    } catch (error) {
        console.error("Error during aggregation process:", error);
        throw new Error("Failed to fetch, aggregate, or save data to the target DB.");
    }
}


// --- AGGREGATION ENDPOINT (Called by the Sync button) ---
app.get('/api/sync', async (req, res) => {
    try {
        const aggregatedData = await fetchAggregateAndSaveData();
        
        // Log the saved data result to the backend console
        console.log("First 2 saved documents (partial view):", aggregatedData.issues.slice(0, 2));

        // Return a summary to the client
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
// (HTML code remains the same as before)
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
                <p>Click the button to fetch and combine all data from the database collections.</p>
                <button id="syncButton">Sync All Data</button>
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
                            status.innerHTML = \`\${data.message}<br>Users: \${data.metadata.totalUsers}, Issues: \${data.metadata.totalIssues}\`;
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
