import React from "react";
import { AppBar, Toolbar, Typography, Button } from "@mui/material";
import { useNavigate } from "react-router-dom";

const Navbar = () => {
  const navigate = useNavigate();

  return (
    <AppBar position="static" color="primary">
      <Toolbar>
        <Typography variant="h6" sx={{ flexGrow: 1 }}>
        Welcome to the product recommendation system
        </Typography>
        <Button color="inherit" onClick={() => navigate("/visualization")}>
        Visualization
        </Button>
      </Toolbar>
    </AppBar>
  );
};

export default Navbar;
