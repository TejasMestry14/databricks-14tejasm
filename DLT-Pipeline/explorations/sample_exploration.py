# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
display(spark.sql("SELECT * FROM workspace.default.walmart_dataset"))