// data_aggregator.js

import express from "express";
import cors from "cors";
import mongoose from "mongoose";
import { config } from "dotenv";

config(); // Load environment variables from .env

const app = express();
const PORT = process.env.PORT || 5002; // Use a different port than your main server

// --- MIDDLEWARE ---
app.use(cors());
app.use(express.json());

// --- DATABASE (MONGODB) ---
mongoose.connect(process.env.MONGO_URI, { dbName: 'janvaani_db' }) // Explicitly set the database name
  .then(() => console.log("✅ MongoDB Aggregator Connected"))
  .catch((err) => console.error("MongoDB Connection Error:", err));

// --- SCHEMAS (Simplified for aggregation) ---

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
    detection: DetectionSubSchema, // Embedded detection data
}, { timestamps: true });

// 3. Flag Schema
const FlagSchema = new mongoose.Schema({
    issueId: { type: mongoose.Schema.Types.ObjectId, ref: 'Issue' },
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
    userName: String,
    type: String, // 'red' or 'green'
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
// Note: We don't need a separate Detection model anymore

// --- AGGREGATION ROUTE ---
app.get('/api/all-data', async (req, res) => {
    try {
        // 1. Fetch all Users
        const users = await User.find({}).select('-otp -otpExpiry -verified -__v').lean();

        // 2. Fetch all Flags
        const flags = await Flag.find({}).select('-__v').lean();

        // 3. Fetch all Issues and embed related data (Flags and User Info)
        const issuesWithDetails = await Issue.aggregate([
            // Stage 1: Lookup the submitting User details
            {
                $lookup: {
                    from: 'users', // The target collection name in MongoDB
                    localField: 'userId',
                    foreignField: '_id',
                    as: 'submittedByUser'
                }
            },
            // Stage 2: De-array the submittedByUser (since userId is unique)
            { $unwind: { path: '$submittedByUser', preserveNullAndEmptyArrays: true } },
            
            // Stage 3: Lookup all Flags for this Issue
            {
                $lookup: {
                    from: 'flags', // The target collection name in MongoDB
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
                    
                    // Image/Detection Swap Logic in Aggregation
                    originalImageUrl: '$imageUrl',
                    imageUrl: { 
                        $ifNull: ['$detection.annotatedImageUrl', '$imageUrl'] 
                    },
                    
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
            issues: issuesWithDetails, // Issues already contain embedded detection and related data
            flags: flags, // Raw flag data (redundant but included for completeness)
        };

        res.json(aggregatedData);

    } catch (error) {
        console.error("Error during data aggregation:", error);
        res.status(500).json({ error: "Server error during data aggregation.", details: error.message });
    }
});


// --- START SERVER ---
app.listen(PORT, () => console.log(`🚀 Data Aggregator running at http://localhost:${PORT}`));
