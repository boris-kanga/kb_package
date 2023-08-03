/*

*/

function KBNotificator(root, config){
	this.root = root || document.body;
	this.conf = config || {position: {x: "right", y: "top"}};
	this.timer = null;
	let self = this;

	this.notify = (message, type="info", delay=1)=>{
		delay = delay * 1000 || "auto";
		
		let container = document.createElement("div");
		container.style="z-index: 10000; position:fixed; right:15px; top: 15px; border-radius:3px; overflow:hidden; background: #50D050; color: #fff; font-family: roboto, sans-serif; box-shadow: 3px 5px 5px #eee"
		container.innerHTML = `
		<div class="progress-bar" style="width: 100%; height: 7px; background: transparent">
			<div class="progresser" style="height:7px; width:0; background: black"></div>
		</div>
		<div style="display:flex; align-items:center; padding: 10px 15px; ">
			<div style="padding-right: 10px; font-size:12px; max-width: 250px;">
				<div style="font-size: 14px">${message.title}</div>
				<div>${message.message}</div>
			</div>
			<div>

				
				<svg style="display:none" width="20" height="20" 
					viewBox="0 0 352.62 352.62">
					<g fill="white">
						<path d="M337.222,22.952c-15.912-8.568-33.66,7.956-44.064,17.748c-23.867,23.256-44.063,50.184-66.708,74.664
							c-25.092,26.928-48.348,53.856-74.052,80.173c-14.688,14.688-30.6,30.6-40.392,48.96c-22.032-21.421-41.004-44.677-65.484-63.648
							c-17.748-13.464-47.124-23.256-46.512,9.18c1.224,42.229,38.556,87.517,66.096,116.28c11.628,12.24,26.928,25.092,44.676,25.704
							c21.42,1.224,43.452-24.48,56.304-38.556c22.645-24.48,41.005-52.021,61.812-77.112c26.928-33.048,54.468-65.485,80.784-99.145
							C326.206,96.392,378.226,44.983,337.222,22.952z M26.937,187.581c-0.612,0-1.224,0-2.448,0.611
							c-2.448-0.611-4.284-1.224-6.732-2.448l0,0C19.593,184.52,22.653,185.132,26.937,187.581z"/>
					</g>
				</svg>
				<svg style="display:none" width="20" height="20"
					viewBox="0 0 512 512" >
					<path fill="white" d="M256,0C114.6,0,0,114.6,0,256s114.6,256,256,256s256-114.6,256-256S397.4,0,256,0z M64,256c0-106.1,86-192,192-192
						c42.1,0,81,13.7,112.6,36.7L100.7,368.6C77.7,337,64,298.1,64,256z M256,448c-42.1,0-81-13.7-112.6-36.7l267.9-267.9
						c23,31.7,36.7,70.5,36.7,112.6C448,362.1,362,448,256,448z"/>
				</svg>

				<svg style="display:none" width="20" height="20" 
					viewBox="0 0 24 24">
					<g fill="white">
						<path d="M11.001 10h2v5h-2zM11 16h2v2h-2z"/>
						<path d="M13.768 4.2C13.42 3.545 12.742 3.138 12 3.138s-1.42.407-1.768 1.063L2.894 18.064a1.986 
								1.986 0 0 0 .054 1.968A1.984 1.984 0 0 0 4.661 21h14.678c.708 0 1.349-.362 1.714-.968a1.989 
								1.989 0 0 0 .054-1.968L13.768 4.2zM4.661 19 12 5.137 19.344 19H4.661z"/>
					</g>
				</svg>

			</div>
		</div>
		<div class="progress-bar" style="width: 100%; height: 7px; display:none;background: transparent">
			<div class="progresser" style="height:7px; width:0"></div>
		</div>
		`;
		let progress_color = "green";
		if (type==="info" || type==="i"){
			container.querySelectorAll("svg")[0].style.display = "block";
		}else if (type==="error" || type==="e"){
			progress_color = "#B22222";
			container.style.background="#DC143C";
			container.querySelectorAll("svg")[1].style.display = "block";
		}else{
			// type === "warning" || type === "w"
			progress_color="#F6BE00"
			container.style.background="#FFD700";
			container.querySelectorAll("svg")[2].style.display = "block";
		}
		self.root.appendChild(container);
		

		if (container.animate){
			container.querySelectorAll(".progresser").forEach((p)=>{
				p.style.background = progress_color;
				p.animate([{"width": "100%"}], {duration:delay});
			});
			self.timer = setTimeout(()=>{
				container.remove();
			}, delay);
		}else{
			let time_position = 0;
			self.timer = setInterval(()=>{

				time_position+=100;
				if (time_position>=delay){
					container.querySelectorAll(".progresser").forEach((p)=>{
						p.style.width = "100%";
					})
					console.log("stop");
					container.remove();
				}else{
					container.querySelectorAll(".progresser").forEach((p)=>{
						p.style.width = (100*time_position/delay)+"%";
					})
				}
			}, 100);		
		}
	};
}