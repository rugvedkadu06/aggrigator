// data_aggregator.js

import express from "express";
import cors from "cors";
import mongoose from "mongoose";
import { config } from "dotenv";

config(); // Load environment variables from .env

const app = express();
const PORT = process.env.PORT || 5002;

// --- MIDDLEWARE ---
app.use(cors());
app.use(express.json());

// --- DATABASE (MONGODB) ---
// Connecting to the 'test' database based on your last provided code snippet
mongoose.connect(process.env.MONGO_URI, { dbName: 'test' }) 
  .then(() => console.log("✅ MongoDB Aggregator Connected (DB: test)"))
  .catch((err) => console.error("MongoDB Connection Error:", err));

// --- SCHEMAS ---

// 1. Detection Sub-Schema (Embedded in Issue)
const DetectionSubSchema = new mongoose.Schema({
    annotatedImageUrl: String,
    detections: Array,
    detectedAt: Date,
}, { _id: false });

// 2. Issue Schema
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
    // If 'priority' and 'assignedTo' are used, they should be included here too, based on image_388447.png
    priority: String, 
    assignedTo: String,
}, { timestamps: true });

// 3. Flag Schema
const FlagSchema = new mongoose.Schema({
    issueId: { type: mongoose.Schema.Types.ObjectId, ref: 'Issue' },
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
    userName: String,
    type: String,
    reason: String,
}, { timestamps: true });

// 4. User Schema (Only fetching a subset of fields)
const UserSchema = new mongoose.Schema({
    name: String,
    email: String,
    phone: String,
    points: Number,
    faceImageUrl: String,
});

// --- MODELS ---
const User = mongoose.model("User", UserSchema);
const Issue = mongoose.model("Issue", IssueSchema);
const Flag = mongoose.model("Flag", FlagSchema);


// --- REUSABLE AGGREGATION FUNCTION ---
async function fetchAndAggregateData() {
    console.log(`\n--- STARTING DATA AGGREGATION at ${new Date().toLocaleTimeString()} ---`);
    
    try {
        // 1. Fetch all Users
        // NOTE: The 'users' collection used here might be in the 'test' database or the default database.
        const users = await User.find({}).select('-otp -otpExpiry -verified -__v').lean();

        // 2. Fetch all Flags
        const flags = await Flag.find({}).select('-__v').lean();

        // 3. Fetch all Issues and embed related data (Flags and User Info)
        const issuesWithDetails = await Issue.aggregate([
            // Stage 1: Lookup the submitting User details (from the 'users' collection)
            {
                $lookup: {
                    from: 'users', // Target collection name
                    localField: 'userId',
                    foreignField: '_id',
                    as: 'submittedByUser'
                }
            },
            // Stage 2: De-array the submittedByUser
            { $unwind: { path: '$submittedByUser', preserveNullAndEmptyArrays: true } },
            
            // Stage 3: Lookup all Flags for this Issue (from the 'flags' collection)
            {
                $lookup: {
                    from: 'flogs', // NOTE: Using 'flogs' if that is the actual collection name for Flags, otherwise use 'flags'
                    localField: '_id',
                    foreignField: 'issueId',
                    as: 'issueFlags'
                }
            },
            
            // Stage 4: Project the final, cleaned data structure
            {
                $project: {
                    // Issue Fields
                    _id: 1, title: 1, description: 1, location: 1,
                    status: 1, redFlags: 1, greenFlags: 1, createdAt: 1,
                    priority: 1, assignedTo: 1,
                    
                    // Image/Detection Swap Logic in Aggregation
                    originalImageUrl: '$imageUrl',
                    imageUrl: { $ifNull: ['$detection.annotatedImageUrl', '$imageUrl'] },
                    
                    // Embedded Detection Data
                    annotatedImageUrl: '$detection.annotatedImageUrl',
                    detections: '$detection.detections',
                    detectedAt: '$detection.detectedAt',

                    // Submitting User Details
                    submittedBy: '$submittedByUser.name',
                    submittedByEmail: '$submittedByUser.email',
                    
                    // Attached Flags
                    flags: '$issueFlags',
                }
            },
            { $sort: { createdAt: -1 } }
        ]);

        // 4. Combine all results into a single object
        const aggregatedData = {
            metadata: {
                timestamp: new Date().toISOString(),
                totalUsers: users.length,
                totalIssues: issuesWithDetails.length,
                totalFlags: flags.length,
            },
            users: users,
            issues: issuesWithDetails,
            flags: flags,
        };
        
        console.log(`--- AGGREGATION COMPLETE --- Total Issues: ${aggregatedData.metadata.totalIssues}`);
        
        return aggregatedData;
        
    } catch (error) {
        console.error("Error during aggregation process:", error);
        throw new Error("Failed to fetch and aggregate data from MongoDB.");
    }
}


// --- AGGREGATION ENDPOINT (Called by the Sync button) ---
app.get('/api/sync', async (req, res) => {
    try {
        const aggregatedData = await fetchAndAggregateData();
        // Log the complete aggregated data to the backend console
        console.log("Full Aggregated Data (partial view):", aggregatedData.issues.slice(0, 2));

        // Return a summary to the client
        res.json({ 
            success: true, 
            message: 'Data sync complete. Results logged to server console.',
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
