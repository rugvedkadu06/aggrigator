// server.js

import express from "express";
import cors from "cors";
import mongoose from "mongoose";
import bodyParser from "body-parser";
import jwt from "jsonwebtoken";
import { v2 as cloudinary } from "cloudinary";
import multer from "multer";
import { config } from "dotenv";

config();

const app = express();
const PORT = process.env.PORT || 5001;

// === PLACEHOLDER CONSTANT ===
const PLACEHOLDER_FACE_IMAGE = "https://picsum.photos/seed/face/200/200";
const PLACEHOLDER_ANNOTATED_IMAGE = "https://picsum.photos/seed/annotated/800/600";
const PLACEHOLDER_ISSUE_IMAGE = "https://picsum.photos/seed/issue/800/600";


// --- MIDDLEWARE ---
app.use(cors());
app.use(bodyParser.json());

// --- DATABASE (MONGODB) ---
mongoose.connect(process.env.MONGO_URI)
  .then(() => console.log("✅ MongoDB Connected"))
  .catch((err) => console.error("MongoDB Connection Error:", err));

// --- SCHEMAS ---
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
  imageUrl: { type: String, default: PLACEHOLDER_ISSUE_IMAGE },
  submittedBy: String,
  redFlags: { type: Number, default: 0 },
  greenFlags: { type: Number, default: 0 },
}, { timestamps: true });

const FlagSchema = new mongoose.Schema({
  issueId: { type: mongoose.Schema.Types.ObjectId, ref: 'Issue', required: true },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
  userName: { type: String, required: true },
  userEmail: { type: String, required: true },
  type: { type: String, enum: ['red', 'green'], required: true },
  reason: { type: String },
}, { timestamps: true });

const DetectionSchema = new mongoose.Schema({
    // Assumed link back to the issue for the aggregation to work
    issueId: { type: mongoose.Schema.Types.ObjectId, ref: 'Issue', required: true },
    annotatedImageUrl: String,
    detections: Array, // Array of detected objects
    createdAt: Date,
});


const User = mongoose.model("User", UserSchema);
const Issue = mongoose.model("Issue", IssueSchema);
const Flag = mongoose.model("Flag", FlagSchema);
const Detection = mongoose.model("Detection", DetectionSchema, 'detections'); // Specify collection name 'detections'


cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

const storage = multer.memoryStorage();
const upload = multer({ storage });

// --- AUTH ROUTES ---
app.post("/api/auth/register-send-otp", async (req, res) => {
  try {
    const { name, email, phone } = req.body;
    if (!email || !name || !phone) return res.status(400).json({ error: "All fields are required" });

    const existingUser = await User.findOne({ email });
    if (existingUser && existingUser.verified) {
      return res.status(400).json({ error: "User with this email already exists." });
    }

    const otp = "124590";
    const otpExpiry = new Date(Date.now() + 10 * 60 * 1000);

    await User.findOneAndUpdate(
      { email },
      { name, phone, otp, otpExpiry, verified: false },
      { upsert: true, new: true }
    );

    res.json({ success: true, message: "OTP process initialized. Use static OTP 124590 to verify." });
  } catch (err) {
    console.error("❌ OTP send error:", err);
    res.status(500).json({ error: "Server error while setting up OTP." });
  }
});

app.post("/api/auth/verify-otp", async (req, res) => {
  try {
    const { email, otp } = req.body;
    const user = await User.findOne({ email });

    if (!user) return res.status(404).json({ error: "User not found." });
    if (user.otp !== otp || new Date() > user.otpExpiry) {
      return res.status(400).json({ error: "Invalid or expired OTP." });
    }

    if (!user.faceImageUrl) {
        user.faceImageUrl = PLACEHOLDER_FACE_IMAGE;
    }

    user.verified = true;
    user.otp = undefined;
    user.otpExpiry = undefined;
    await user.save();

    const token = jwt.sign({ id: user._id }, process.env.JWT_SECRET, { expiresIn: '30d' });

    res.json({
      success: true,
      token,
      user: { id: user._id, name: user.name, email: user.email, phone: user.phone, points: user.points, faceImageUrl: user.faceImageUrl }
    });
  } catch (err) {
    res.status(500).json({ error: "Server error during verification." });
  }
});

// --- JWT AUTH MIDDLEWARE ---
const authMiddleware = (req, res, next) => {
  const token = req.header('Authorization')?.replace('Bearer ', '');
  if (!token) return res.status(401).json({ error: 'Access denied. No token provided.' });
  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    req.userId = decoded.id;
    next();
  } catch (ex) {
    res.status(400).json({ error: 'Invalid token.' });
  }
};

// --- PROTECTED USER ROUTES ---
app.post('/api/user/upload-face', authMiddleware, upload.single('faceImage'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No image file provided.' });
  try {
    const result = await new Promise((resolve, reject) => {
      const uploadStream = cloudinary.uploader.upload_stream({ folder: "janvaani_faces" }, (error, result) => {
        if (error) reject(error); else resolve(result);
      });
      uploadStream.end(req.file.buffer);
    });
    
    const user = await User.findByIdAndUpdate(req.userId, { faceImageUrl: result.secure_url }, { new: true });
    res.json({ success: true, message: 'Face image updated!', faceImageUrl: user.faceImageUrl });
  } catch (err) {
    res.status(500).json({ error: 'Image upload failed.' });
  }
});

app.get('/api/user/me', authMiddleware, async (req, res) => {
    const user = await User.findById(req.userId).select('-otp -otpExpiry');
    res.json(user);
});

// --- PROTECTED ISSUE ROUTES ---

app.get('/api/issues', authMiddleware, async (req, res) => {
    const { filter } = req.query;
    let matchQuery = {};
    
    if (filter !== 'active') { // If filter is not 'active', show only user's own issues
        matchQuery = { userId: new mongoose.Types.ObjectId(req.userId) };
    }

    try {
        const issues = await Issue.aggregate([
            // 1. Filter issues based on the user or 'active' status
            { $match: matchQuery },

            // 2. Perform a left join with the 'detections' collection
            {
                $lookup: {
                    from: 'detections',        // The collection to join with (collection name)
                    localField: '_id',         // Field from the input documents (Issue's _id)
                    foreignField: 'issueId',   // Field from the documents of the 'detections' collection
                    as: 'detectionData'        // Name of the new array field to add to the Issue documents
                }
            },

            // 3. Project the final structure (pulling out the needed detection fields)
            {
                $project: {
                    // Include all existing Issue fields
                    _id: 1, userId: 1, title: 1, description: 1, location: 1,
                    coordinates: 1, status: 1, imageUrl: 1, submittedBy: 1,
                    redFlags: 1, greenFlags: 1, createdAt: 1, updatedAt: 1,
                    
                    // Add the 'annotatedImageUrl' field from the first detection result
                    annotatedImageUrl: { $ifNull: [{ $arrayElemAt: ['$detectionData.annotatedImageUrl', 0] }, null] },
                    
                    // Add the 'detections' array from the first detection result
                    detections: { $ifNull: [{ $arrayElemAt: ['$detectionData.detections', 0] }, []] }
                }
            },
            
            // 4. Sort the results
            { $sort: { createdAt: -1 } },
        ]);
        
        res.json(issues);
    } catch (error) {
        console.error("Error fetching issues:", error);
        res.status(500).json({ error: "Server error fetching issues." });
    }
});


app.post('/api/issues', authMiddleware, upload.single('issueImage'), async (req, res) => {
    const { title, description, location, submittedBy, latitude, longitude } = req.body;
    let imageUrl = "";
    
    if (req.file) {
        try {
            const result = await new Promise((resolve, reject) => {
                const uploadStream = cloudinary.uploader.upload_stream({ 
                    folder: "janvaani_issues",
                    transformation: [
                        { width: 800, height: 600, crop: "fill", quality: "auto" }
                    ]
                }, (error, result) => {
                    if (error) reject(error); else resolve(result);
                });
                uploadStream.end(req.file.buffer);
            });
            imageUrl = result.secure_url;
        } catch (uploadError) {
             console.error("Cloudinary upload error:", uploadError);
             return res.status(500).json({ error: "Image upload failed to Cloudinary." });
        }
    }
    
    const issueData = { 
        userId: req.userId, 
        title, 
        description, 
        location, 
        submittedBy, 
        imageUrl: imageUrl || PLACEHOLDER_ISSUE_IMAGE // Use placeholder if no file uploaded
    };
    
    if (latitude && longitude) {
        issueData.coordinates = {
            latitude: parseFloat(latitude),
            longitude: parseFloat(longitude)
        };
    }
    
    try {
        const issue = new Issue(issueData);
        await issue.save();
        res.status(201).json(issue);
    } catch (dbError) {
        console.error("Database save error:", dbError);
        res.status(500).json({ error: "Failed to save issue to database." });
    }
});

app.post('/api/issues/:issueId/flag', authMiddleware, async (req, res) => {
    const { issueId } = req.params;
    const { flagType, reason } = req.body;
    const userId = req.userId;

    try {
        const issue = await Issue.findById(issueId);
        if (!issue) {
            return res.status(404).json({ message: 'Issue not found' });
        }

        if (issue.userId.toString() === userId) {
            return res.status(400).json({ message: 'You cannot flag your own issue' });
        }

        const existingFlag = await Flag.findOne({ issueId, userId });
        if (existingFlag) {
            return res.status(400).json({ message: 'You have already flagged this issue' });
        }
        
        const user = await User.findById(userId).select('name email');
        if (!user) {
            return res.status(404).json({ message: 'Flagging user not found' });
        }

        if (flagType === 'red') {
            issue.redFlags++;
        } else if (flagType === 'green') {
            issue.greenFlags++;
        } else {
            return res.status(400).json({ message: 'Invalid flag type' });
        }

        const flag = new Flag({
            issueId,
            userId,
            userName: user.name, 
            userEmail: user.email, 
            type: flagType,
            reason: flagType === 'red' ? reason : null,
        });

        await flag.save();
        await issue.save();

        res.status(200).json({ message: 'Issue flagged successfully', issue });
    } catch (error) {
        console.error("Flagging error:", error);
        res.status(500).json({ message: 'Server error', error: error.message });
    }
});


// === MOCK ROUTES FOR TESTING IMAGE/DETECTION ===

app.post('/api/mock/detection', authMiddleware, async (req, res) => {
    const { issueId } = req.body;

    if (!issueId || !mongoose.Types.ObjectId.isValid(issueId)) {
        return res.status(400).json({ message: 'Valid issueId is required' });
    }

    try {
        const existingDetection = await Detection.findOne({ issueId });
        if (existingDetection) {
            return res.status(400).json({ message: 'Detection already exists for this issue' });
        }
        
        const issue = await Issue.findById(issueId);
        if (!issue) {
            return res.status(404).json({ message: 'Issue not found' });
        }

        const newDetection = new Detection({
            issueId,
            annotatedImageUrl: PLACEHOLDER_ANNOTATED_IMAGE,
            detections: [
                { class: 'pothole', confidence: 0.95, box: [100, 100, 200, 200] },
                { class: 'garbage_pile', confidence: 0.88, box: [500, 300, 700, 500] }
            ],
            createdAt: new Date(),
        });

        await newDetection.save();
        res.status(201).json({ 
            message: 'Mock Detection created successfully. Now check /api/issues.',
            detection: newDetection
        });

    } catch (error) {
        console.error("Mock Detection Error:", error);
        res.status(500).json({ message: 'Server error creating mock detection', error: error.message });
    }
}); // <--- FIX 1: Missing closing brace and parenthesis here

// --- ROOT ROUTE ---
app.get('/', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <title>Janvaani Backend API</title>
            <style>body { font-family: sans-serif; text-align: center; padding-top: 50px; background-color: #f4f4f9; } h1 { color: #333; } p { color: #666; }</style>
        </head>
        <body>
            <h1>Janvaani Backend API</h1>
            <p>Server is running successfully!</p>
            <p>API is available on port ${PORT}</p>
        </body>
        </html>
    `);
});


// --- START SERVER ---
app.listen(PORT, () => console.log(`🚀 Server running at http://localhost:${PORT}`));
