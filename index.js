const express = require("express");
const { startRabbitMqConsumer } = require("./rabbitMqConsumer");
const { sendMessage } = require("./rabbitMqHelper");
const multer = require("multer");
const fs = require("fs");
const excelReader = require("xlsx");
require("dotenv").config();

const app = express();
app.use(express.json(), express.urlencoded({ extended: true }));

const PORT = 3005;

const uploadMulter = multer({ dest: 'uploads/' });

const unlinkFile = (filePath) => {
  try {
    fs.unlinkSync(filePath);
  } catch (error) {
    console.error("Has error when deleting uploaded file: ", { context: error });
  }
};

app.post("/upload", uploadMulter.single("file"), async (req, res) => {
  const { file } = req;
  const excelFile = excelReader.readFile(file.path);
  const uploadRooms = excelReader.utils.sheet_to_json(excelFile.Sheets[excelFile.SheetNames[0]]);
  unlinkFile(file.path);
  await sendMessage({ message: uploadRooms }, process.env.UPLOAD_FILE_QUEUE);
  res.json("upload file excel OK");
});

app.listen(PORT, () => {
  console.log(`Server is running at: http://localhost:${PORT}`);
});

// Start RabbitMQ Consumer
startRabbitMqConsumer();
