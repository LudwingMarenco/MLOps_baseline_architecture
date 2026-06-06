# Scalers
from sklearn.ensemble import (
    ExtraTreesClassifier,
    HistGradientBoostingClassifier,
    RandomForestClassifier,
)
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import MinMaxScaler, RobustScaler, StandardScaler

# Classifiers
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier

scalers = {
    "minmaxscaler": MinMaxScaler(),
    "standardscaler": StandardScaler(),
    "robustscaler": RobustScaler(),
    "passthrough": None,
}

classifiers = {
    "randomforest": RandomForestClassifier,
    "histgradientboosting": HistGradientBoostingClassifier,
    "svc": SVC,
    "kneighbors": KNeighborsClassifier,
    "decisiontree": DecisionTreeClassifier,
    "mlp": MLPClassifier,
    "extratrees": ExtraTreesClassifier,
}
