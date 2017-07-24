function [z,J_his] = descentFunction(z,X,y,a,iters)
	J_his = zeros(iters,1);
	m = length(y);
	n = length(z);
	t = zeros(n,1);
	for iter = 1:iters
		for i = 1:n
			t(i) = (a/m)*(X*z-y)'*X(:,i);
		end;
		for i = 1:n
			z(i) = z(i) - t(i);
		end;
		J_his(iter) = sum((X*z-y).^2)/(2*m);
	end
end
