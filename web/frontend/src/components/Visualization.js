import React, { useEffect, useState } from "react";
import { Bar, Pie } from "react-chartjs-2";
import { Box, Grid, Typography } from "@mui/material";
import axios from "axios"; 
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

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  ArcElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

// const Visualization = () => {
//   const [barData1, setBarData1] = useState(null);
//   const [barData2, setBarData2] = useState(null);
//   const [pieData, setPieData] = useState(null);
//   const [lineData, setLineData] = useState(null);

const Visualization = () => {
  const [monthlySales, setMonthlySales] = useState(null);
  const [topSpendingCustomers, setTopSpendingCustomers] = useState(null);
  const [topSellingCategories, setTopSellingCategories] = useState(null);
  const [topSpendingTrends, setTopSpendingTrends] = useState(null);

  // Request data
  const fetchData = async () => {
    try {
      const topCatResponse = await axios.get("/api/stat/top_category");
      const topUserResponse = await axios.get("/api/stat/top_user");
      const monthlySalesResponse = await axios.get("/api/stat/per_month");
      const topCatTrendResponse = await axios.get("/api/stat/top_category_per_month");

      const topCat = topCatResponse.data
      const topUser = topUserResponse.data
      const monthlySales = monthlySalesResponse.data
      const topCatTrend = topCatTrendResponse.data

      monthlySales.datasets[0].backgroundColor = "#FF6384";
      monthlySales.datasets[0].yAxisID = "y1";
      monthlySales.datasets[1].backgroundColor = "#36A2EB";
      monthlySales.datasets[1].yAxisID = "y2";
      topUser.datasets[0].backgroundColor = "#FFCE56";
      topUser.datasets[0].yAxisID = "y1";
      topUser.datasets[1].backgroundColor = "#8E24AA";
      topUser.datasets[1].yAxisID = "y2";
      topCat.datasets[0].backgroundColor = [
        "#FF6384", "#36A2EB", "#FFCE56", "#4BC0C0",
        "#9966FF", "#FF9F40", "#FF6347", "#98FB98",
        "#FFD700", "#FF1493", "#C71585", "#8A2BE2",
        "#A52A2A", "#B0C4DE", "#DEB887", "#7CB342"
      ];
      topCatTrend.datasets[0].backgroundColor = "#B481BB";
      topCatTrend.datasets[0].yAxisID = "y1";
      topCatTrend.datasets[1].backgroundColor = "#2596BE";
      topCatTrend.datasets[1].yAxisID = "y2";

      setMonthlySales(monthlySales);
      setTopSpendingCustomers(topUser);
      setTopSellingCategories(topCat);
      setTopSpendingTrends(topCatTrend);
    } catch (error) {
      console.error("Error fetching data:", error);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  if (!monthlySales || !topSpendingCustomers || !topSellingCategories || !topSpendingTrends) {
    return <Typography variant="h6" textAlign="center">Loading...</Typography>;
  }

  // Chart Options
  const options = {
    plugins: {
      legend: {
        position: "top",
      },
    },
    maintainAspectRatio: false,
  };

  const twoAxisOptions = {
    plugins: {
      legend: {
        position: "top",
      },
    },
    maintainAspectRatio: false,
    scales: {
      y1: {
        type: "linear",
        position: "left",
        title: {
          display: true,
          text: "Monthly Sales",
        },
      },
      y2: {
        type: "linear",
        position: "right",
        grid: {
          drawOnChartArea: false,
        },
        title: {
          display: true,
          text: "Quantity Sold",
        },
      },
    },
  }

  return (
    <Box p={3}>
      <Typography variant="h4" textAlign="center" gutterBottom>
        Data Visualization
      </Typography>
      <Grid container spacing={3}>
        {/* Monthly Sales and Total Price */}
        <Grid item xs={12} md={6}>
          <Box height={300}>
            <Typography variant="h6" textAlign="center">
              Sales Information Each Month in 2020
            </Typography>
            <Bar data={monthlySales} options={twoAxisOptions}/>
          </Box>
        </Grid>

        {/* Top 10 Spending Customers */}
        <Grid item xs={12} md={6}>
          <Box height={300}>
            <Typography variant="h6" textAlign="center">
              Top 10 Customers Who Spent the Most
            </Typography>
            <Bar data={topSpendingCustomers} options={{
               plugins: {
                 legend: {
                   position: "top",
                 },
               },
               maintainAspectRatio: false,
               scales: {
                 y1: {
                   type: "linear",
                   position: "left",
                   title: {
                     display: true,
                     text: "Total Spending",
                   },
                 },
                 y2: {
                   type: "linear",
                   position: "right",
                   grid: {
                     drawOnChartArea: false,
                   },
                   title: {
                     display: true,
                     text: "Quantity Purchased",
                   },
                 },
               },
             }} />
          </Box>
        </Grid>

        {/* Top 15 Selling Products */}
        <Grid item xs={12} md={6} sx={{ mt: 4 }}>
          <Box height={300}>
            <Typography variant="h6" textAlign="center">
              Top 15 Best-Selling Categories
            </Typography>
            <Pie data={topSellingCategories} options={options} />
          </Box>
        </Grid>

        {/* Top 1 Spending Trend */}
        <Grid item xs={12} md={6} sx={{ mt: 4 }}>
          <Box height={300}>
            <Typography variant="h6" textAlign="center">
              Top Category Monthly Sales
            </Typography>
            <Bar data={topSpendingTrends} options={twoAxisOptions} />
          </Box>
        </Grid>
      </Grid>
    </Box>
  );
};

export default Visualization;
