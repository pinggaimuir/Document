function Z = costFunction(z0,z1,X,y)
	Z = zeros(length(z0),length(z1));
	m = length(y);
	for i=1:length(z0)
		for j=1:length(z1)
			Z(i,j) = sum((X*[z0(i);z1(j)]-y).^2)/(2*m);
		end
	end
	Z = Z';
end
