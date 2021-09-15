// JavaScript source code
var X = '<form action="index.html" ><input type="submit" class="history_button" value="2021 /         " /></form><form action="html/2020_2021/index.html"><input type="submit" class="history_button" value="2020 / 2021" /></form><form action="html/2019_2020/index.html"><input type="submit" class="history_button" value="2019 / 2020" /></form>';
//document.getElementById("FromScript").innerHTML = 3;



	//<form action = "index.html">
	//	<input type="submit" class="history_button" value="2021 /     " />
	//</form>
	//<form action="html/2020_2021/index.html">
	//	<input type="submit" class="history_button" value="2020 / 2021" />
	//</form>
	//<form action="html/2019_2020/index.html">
	//	<input type="submit" class="history_button" value="2019 / 2020" />
	//</form>

console.log(X);

$ajaxUtils
	.sendGetRequest("2019_2020/index.html",
		function (request) {
			var name = request.responseText;

			//document.querySelector("#content")
			//	.innerHTML = "<h2>Hello " + name + "!</h2>";
			console.log(name);
		});


console.log("asdasd");