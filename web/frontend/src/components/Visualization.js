import React, { useEffect, useState } from "react";
import { Bar, Pie, Line } from "react-chartjs-2";
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

// 注册 ChartJS 插件
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

      setMonthlySales(monthlySalesResponse.data);
      setTopSpendingCustomers(topUserResponse.data);
      setTopSellingCategories(topCatResponse.data);
      setTopSpendingTrends(topCatTrendResponse.data);
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
              2020 每月销售和总价格
            </Typography>
            <Bar data={monthlySales} options={options} />
          </Box>
        </Grid>

        {/* Top 10 Spending Customers */}
        <Grid item xs={12} md={6}>
          <Box height={300}>
            <Typography variant="h6" textAlign="center">
              Top 10 花钱最多的客户
            </Typography>
            <Bar data={topSpendingCustomers} options={options} />
          </Box>
        </Grid>

        {/* Top 15 Selling Products */}
        <Grid item xs={12} md={6} sx={{ mt: 4 }}>
          <Box height={300}>
            <Typography variant="h6" textAlign="center">
              Top 15 卖得最好的商品
            </Typography>
            <Pie data={topSellingCategories} options={options} />
          </Box>
        </Grid>

        {/* Top 1 Spending Trend */}
        <Grid item xs={12} md={6} sx={{ mt: 4 }}>
          <Box height={300}>
            <Typography variant="h6" textAlign="center">
              Top 1 客户每月花费趋势
            </Typography>
            <Line data={topSpendingTrends} options={options} />
          </Box>
        </Grid>
      </Grid>
    </Box>
  );
};

export default Visualization;
