import React, { useState } from "react";
import { Box, TextField, Button, Typography, Card, CardContent } from "@mui/material";
import axios from "axios";

const HomePage = () => {
  const [userId, setUserId] = useState("");
  const [recommendations, setRecommendations] = useState([]);

  const fetchRecommendations = async () => {
    try {
      const response = await axios.get(`/api/user_predict/${userId}`);
      setRecommendations(response.data);
    } catch (error) {
      console.error("Error fetching recommendations:", error);
    }
  };

  return (
    <Box
      display="flex"
      flexDirection="column"
      alignItems="center"
      justifyContent="center"
      minHeight="80vh"
      p={3}
    >
      <Typography variant="h4" gutterBottom>
        View Recommendations
      </Typography>
      <TextField
        label="User ID"
        variant="outlined"
        value={userId}
        onChange={(e) => setUserId(e.target.value)}
        sx={{ mb: 2, width: "300px" }}
      />
      <Button
        variant="contained"
        color="primary"
        onClick={fetchRecommendations}
        sx={{ mb: 3 }}
      >
        Enter user ID to see recommended products
      </Button>
      <Box>
        {recommendations.length > 0 &&
          recommendations.map((product, index) => (
            <Card key={index} sx={{ mb: 2, width: "300px" }}>
              <CardContent>
                <Typography variant="h6">{product.name}</Typography>
              </CardContent>
            </Card>
          ))}
      </Box>

    </Box>
  );
};

export default HomePage;
