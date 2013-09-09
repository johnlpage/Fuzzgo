function FrontpageCtrl($scope,$http) {
	
  $scope.explainplan = { millis: 0 , efficiency: 100, n: 0};
  $scope.combiner = "all";
  $scope.nmatch = 2;
  $scope.hasResults = false;
  $scope.permute="exact";
  $scope.results = [];
  
  $scope.doSearch = function() {
	 params = new Object();
	 $scope.results= {};
	 $scope.hasResults=false;
	 $scope.isSearching = true;
	 params["firstname"]=$scope.firstname;
	 params["middlenameone"]=$scope.middlenameone;
	 params["middlenametwo"]=$scope.middlenametwo;
	 params["lastname"]=$scope.lastname;
	 params["address"]=$scope.address;
	 params["phone"]=$scope.phone;
	 params["combiner"]=$scope.combiner;
	 params["permute"]=$scope.permute;
	 params["nmatch"]=$scope.nmatch;
	 params["anyfield"]=$scope.anyfield;
	 
	
	 
	  $http.get('search/simple',{"params":params}).success(function(data) {
		  	$scope.results= data.results;
		  	$scope.explainplan = data.explain;
		  	efval = (100 * data.explain.n)/ data.explain.nscanned;
		  	efval = Math.round(efval * 1000)/1000;
		  	$scope.explainplan.efficiency = efval;
		  	$scope.isSearching = false;
		  	$scope.hasResults = true;
		  });
	  
  }
};
