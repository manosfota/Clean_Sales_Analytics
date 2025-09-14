# Import Libraries
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report


#==========================================
# Step 1: Data Loading & Initial Inspection
#==========================================

print("=" * 50)
print("STEP 1: DATA LOADING & INITIAL INSPECTION")
print("=" * 50)

train_df=pd.read_csv(r'C:\Users\fotam\OneDrive\Υπολογιστής\Exploratory_Data_Analysis\data\train.csv')
print(f"\nFirst 15 rows: \n {train_df.head(15)}")
print(f"\nDataset shape: \n {train_df.shape} ")
print(f"\nMissing values per column: \n{train_df.isnull().sum()}")
print(f"\nData types:\n {train_df.dtypes}")
print(f"\nSummary Statistics: \n {train_df.describe()}")


#======================
# Step 2: Data Cleaning
#======================

print("\n" + "=" * 50)
print("STEP 2: DATA CLEANING")
print("=" * 50)

train_df['Age'].fillna(train_df['Age'].median(), inplace=True)
train_df.drop('Cabin', axis=1, inplace=True)
train_df['Embarked'].fillna(train_df['Embarked'].mode()[0],inplace=True)
# Convert gender to binary
train_df['Sex']=train_df['Sex'].map({'male':0, 'female':1})
# Convert Embarked to codes
train_df['Embarked']=train_df['Embarked'].map({'S':0, 'C':1, 'Q':2})
# Create family size feature
train_df['Family_Size']=train_df['SibSp'] + train_df['Parch']
# Create is_alone flag
train_df['Is_Alone']=(train_df['Family_Size']==0).astype(int)


#============================
# Step 3: Feature Engineering
#============================

print("\n" + "=" * 50)
print("STEP 3: FEATURE ENGINEERING")
print("=" * 50)

# Extract titles from names
train_df['Title']=train_df['Name'].str.extract('([A-Za-z]+)\.',expand=False)
train_df['Title'] = train_df['Title'].replace(['Lady','Countess',
  'Capt','Col','Don','Dr','Major','Rev','Sir','Jonkheer','Dona'], 'Rare')
train_df['Title']=train_df['Title'].replace('Mlle','Miss')
train_df['Title']=train_df['Title'].replace('Ms', 'Miss')
train_df['Title']=train_df['Title'].replace('Mme', 'Mrs')


#=============================
# Step 4: Basic Visualizations
#=============================

print("\n" + "=" * 50)
print("STEP 4: BASIC VISUALIZATIONS")
print("=" * 50)

# Survival by gender
sns.barplot(x='Sex', y='Survived', data=train_df)
plt.title("Survival Rate by Ticket Class")
plt.show()

# Survival by class
sns.barplot(x='Pclass', y='Survived', data=train_df)
plt.title("Survival Rate by Ticket Class")
plt.show()

#================================
# Step 5: Advanced Visualizations
#================================

print("\n" + "=" * 50)
print("STEP 5: ADVANCED VISUALIZATIONS")
print("=" * 50)

# Correlation heatmap
plt.figure(figsize=(12,8))
sns.heatmap(train_df.select_dtypes(include='number').corr(),
            annot=True, cmap='coolwarm', fmt=".2f")
plt.title("Feature Correlation Matrix")
plt.tight_layout()
plt.show()

# Age distribution by survival
plt.figure(figsize=(10,6))
sns.histplot(data=train_df, x='Age', hue='Survived', bins=30, kde=True, alpha=0.6)
plt.title("Age Distribution by Survival Status")
plt.xlabel("Age")
plt.ylabel("Count")
plt.show()

# Fare distribution by class
plt.figure(figsize=(10,6))
sns.boxplot(x='Pclass', y='Fare', data=train_df)
plt.title("Fare Distribution by Passenger Class!")
plt.show()


#==============================
# Step 6: Machine Learning Prep
#==============================

print("\n" + "=" * 50)
print("STEP 6: MACHINE LEARNING PREPARATION")
print("=" * 50)

# Select features for ML model
features=['Pclass', 'Sex', 'Age', 'Family_Size', 'Fare']
X=train_df[features]
y=train_df['Survived']

print(f"Feature shape: {X.shape}" )
print(f"Target shape: {y.shape}" )
print(f"Features used: {features}" )

# Scale numerical features
print("Scaling numerical features...")
scaler=StandardScaler()
X.loc[:,['Age','Fare']]= scaler.fit_transform(X[['Age','Fare']])

print("First 5 rows of scaled features: ")
print(X.head())
 

# ==============================
# Step 7: Machine Learning Model
# ==============================

print("\n" + "=" * 50)
print("STEP 7: MACHINE LEARNING MODEL")
print("=" * 50)

# Split the data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y) 
print(f"Training set: {X_train.shape[0]} samples")
print(f"Testing  set: {X_test. shape[0]} samples")

# Create and train logistic regression model
model = LogisticRegression(random_state=42, max_iter=1000)
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)
y_pred_proba = model.predict_proba(X_test)[:, 1]  # Probability of survival


# ==============================================
# Step 8: Model Evaluation
# ==============================================

print("\n" + "=" * 50)
print("STEP 8: MODEL EVALUATION")
print("=" * 50)

# Calculation accuracy
accuracy=accuracy_score(y_test, y_pred)
print(f"Model Accuracy :{accuracy: .4f} ({accuracy*100:.2f}%)")
 
# Detailed classification report
print("\nClassification Report:")
print(classification_report(y_test, y_pred, target_names=['Did Not Survive', 
                                                          'Survived']))

# Feature importance
feature_importance = pd.DataFrame({
    'Feature': features,
    'Coefficient': model.coef_[0],
    'Absolute_Importance': abs(model.coef_[0])
}).sort_values('Absolute_Importance', ascending=False)

print("\nFeature Importance:")
print(feature_importance)


# ==============================================
# Step 9: Save Results
# ==============================================

print("\n" + "=" * 50)
print("STEP 9: SAVE RESULTS")
print("=" * 50)

# Save cleaned dataset
train_df.to_csv('cleaned_titanic.csv', index=False)
print("Cleaned dataset saved as 'cleaned_titanic.csv'")

# Create predictions DataFrame
results_df = X_test.copy()
results_df['Actual_Survived'] = y_test.values
results_df['Predicted_Survived'] = y_pred
results_df['Survival_Probability'] = y_pred_proba

print("\nFirst 10 predictions:")
print(results_df[['Actual_Survived', 'Predicted_Survived', 'Survival_Probability']].head(10))

# ==============================================
# Final Summary
# ==============================================

print("\n" + "=" * 50)
print("PROJECT SUMMARY")
print("=" * 50)
print(f"✓ Dataset cleaned and prepared")
print(f"✓ {len(features)} features used for modeling")
print(f"✓ Logistic Regression model trained")
print(f"✓ Model accuracy: {accuracy*100:.2f}%")
print(f"✓ Most important feature: {feature_importance['Feature'].iloc[0]}")
print(f"✓ Results saved to file")
print("=" * 50)