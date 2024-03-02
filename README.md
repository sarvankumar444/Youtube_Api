
# YouTube Data Migration App

This application allows you to migrate data from YouTube to MongoDB and PostgreSQL databases. It utilizes the YouTube Data API to fetch channel details, playlists, videos, and comments.

## Prerequisites

Before running the application, ensure you have the following:

- Google API key with access to the YouTube Data API.
- MongoDB Atlas account for storing data in MongoDB.
- PostgreSQL installed locally or on a remote server.
- Streamlit library installed (`pip install streamlit`).

## Setup

1. Clone the repository to your local machine:

```
git clone https://github.com/your_username/YouTube-Data-Migration-App.git
```

2. Navigate to the project directory:

```
cd YouTube-Data-Migration-App
```

3. Install the required Python packages:

```
pip install -r requirements.txt
```

4. Replace the placeholders in the `app.py` file with your Google API key, MongoDB connection string, and PostgreSQL credentials.

## Running the App

To run the application, execute the following command:

```
streamlit run app.py
```

This will launch the Streamlit app in your default web browser. You can then interact with the app to migrate data from YouTube to MongoDB and PostgreSQL, list channels, and execute SQL queries.

## Features

- Copy data from YouTube to MongoDB and PostgreSQL databases.
- List available YouTube channels.
- Execute SQL queries to retrieve insights from the migrated data.
- User-friendly interface for easy interaction.

## Contributors

- Name : Sarvan Kumar M
- Email : soulsarvankumar007@gmail.com

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
