import React from "react";
import { Bar, Pie, Line } from "react-chartjs-2";
import { Box, Grid, Typography } from "@mui/material";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  ArcElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from "chart.js";

// 注册 ChartJS 插件
ChartJS.register(CategoryScale, LinearScale, BarElement, ArcElement, PointElement, LineElement, Title, Tooltip, Legend);

const barData1 = {
  labels: ["Product A", "Product B", "Product C", "Product D"],
  datasets: [
    {
      label: "Sales Count",
      data: [12, 19, 3, 5],
      backgroundColor: ["#FF6384", "#36A2EB", "#FFCE56", "#4BC0C0"],
    },
  ],
};

const barData2 = {
  labels: ["Region 1", "Region 2", "Region 3", "Region 4"],
  datasets: [
    {
      label: "Region Sales",
      data: [22, 10, 14, 7],
      backgroundColor: ["#FF6384", "#36A2EB", "#FFCE56", "#9966FF"],
    },
  ],
};

const pieData = {
  labels: ["Category A", "Category B", "Category C"],
  datasets: [
    {
      data: [300, 50, 100],
      backgroundColor: ["#FF6384", "#36A2EB", "#FFCE56"],
    },
  ],
};

const lineData = {
  labels: ["January", "February", "March", "April", "May", "June", "July"],
  datasets: [
    {
      label: "Monthly Purchases",
      data: [65, 59, 80, 81, 56, 55, 40],
      fill: false,
      borderColor: "#36A2EB",
      tension: 0.1,
    },
  ],
};

// Chart Options
const options = {
  plugins: {
    legend: {
      position: "top",
    },
  },
  maintainAspectRatio: false,
};

const Visualization = () => {
  return (
    <Box p={4}>
      <Typography variant="h5" textAlign="center" gutterBottom sx={{ fontSize: "1.5rem", fontWeight: "bold" }}>
        Data Visualization
      </Typography>
      <Grid container spacing={5}>
        {/* first */}
        <Grid item xs={12} md={6}>
          <Box height={300} sx={{ padding: 2, border: "1px solid #ddd", borderRadius: "8px" }}>
            <Typography variant="h6" textAlign="center" sx={{ fontSize: "1rem", marginBottom: 2 }}>
              prodct sales
            </Typography>
            <Bar data={barData1} options={options} />
          </Box>
        </Grid>

        {/* second */}
        <Grid item xs={12} md={6}>
          <Box height={300} sx={{ padding: 2, border: "1px solid #ddd", borderRadius: "8px" }}>
            <Typography variant="h6" textAlign="center" sx={{ fontSize: "1rem", marginBottom: 2 }}>
              region sales
            </Typography>
            <Bar data={barData2} options={options} />
          </Box>
        </Grid>

        {/* pie */}
        <Grid item xs={12} md={6}>
          <Box sx={{ padding: 2, border: "1px solid #ddd", borderRadius: "8px" }}>
            <Typography 
              variant="h6" 
              sx={{ fontSize: "1rem", marginBottom: 2, textAlign: "left" }}>
              product occupay
            </Typography>
            <Box sx={{ position: "relative", height: 250, width: "100%" }}>
              <Pie data={pieData} options={options} />
            </Box>
          </Box>
        </Grid>


        {/* line  */}
        <Grid item xs={12} md={6}>
          <Box height={300} sx={{ padding: 2, border: "1px solid #ddd", borderRadius: "8px" }}>
            <Typography variant="h6" textAlign="center" sx={{ fontSize: "1rem", marginBottom: 2 }}>
              buy qualities per month
            </Typography>
            <Line data={lineData} options={options} />
          </Box>
        </Grid>
      </Grid>
    </Box>
  );
};

export default Visualization;
